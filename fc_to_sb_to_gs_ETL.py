# ETL listone Fantacalcio + crediti

import os
import time
import warnings
import logging
import shutil
import pandas as pd
import psycopg2
import requests
import gspread
from gspread_dataframe import set_with_dataframe
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import sys

sys.stdout.reconfigure(line_buffering=True)
warnings.filterwarnings("ignore")

# === CONFIGURAZIONE DA ENV / SECRETS ===
SUPABASE_HOST = os.environ.get("SUPABASE_HOST", "aws-1-eu-central-1.pooler.supabase.com")
SUPABASE_PORT = int(os.environ.get("SUPABASE_PORT", 6543))
SUPABASE_DB = os.environ.get("SUPABASE_DB", "postgres")
SUPABASE_USER = os.environ.get("SUPABASE_USER", "postgres.vhowswomnwhbfdpslsep")
SUPABASE_PASSWORD = os.environ.get("SUPABASE_PASSWORD")  
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "giocatore")
SUPABASE_TABLE_CREDITI = os.environ.get("SUPABASE_TABLE_CREDITI", "squadra")
SUPABASE_TABLE_MOVIMENTI = os.environ.get("SUPABASE_TABLE_MOVIMENTI", "movimenti_squadra")
SUPABASE_TABLE_ASTE = os.environ.get("SUPABASE_TABLE_ASTE", "asta")

FANTACALCIO_USERNAME = os.environ.get("FANTACALCIO_USERNAME", "mura88")
FANTACALCIO_PASSWORD = os.environ.get("FANTACALCIO_PASSWORD")

# Optional: JSON content for Google credentials
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDENTIALS_PATH = os.path.join(os.getcwd(), "google_credentials.json")
if GOOGLE_CREDENTIALS_JSON:
    with open(GOOGLE_CREDENTIALS_PATH, "w", encoding="utf-8") as f:
        f.write(GOOGLE_CREDENTIALS_JSON)
    
if not SUPABASE_PASSWORD:
    raise RuntimeError("Missing SUPABASE_PASSWORD environment variable (set as GitHub Secret)")
if not FANTACALCIO_PASSWORD:
    raise RuntimeError("Missing FANTACALCIO_PASSWORD environment variable (set as GitHub Secret)")

# === PATHS ===
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
TARGET_FILE = os.path.join(DOWNLOAD_DIR, "listone.xlsx")

# === LOGGING ===
log_file = os.path.join(os.getcwd(), "log.txt")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# === FUNZIONE DOWNLOAD LISTONE FANTACALCIO ===
def scarica_listone():
    """Scarica il listone Fantacalcio gestendo l'apertura del modal di login o bypassando se accessibile"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-agent={USER_AGENT}")

    chromium_path = shutil.which("chromium") or shutil.which("chromium-browser") or shutil.which("google-chrome")
    if chromium_path:
        options.binary_location = chromium_path

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get("https://www.fantacalcio.it/login")
        wait = WebDriverWait(driver, 20)
        time.sleep(3)

        # 1. Rimuovi o accetta banner Cookie (Iubenda)
        try:
            cookie_btn = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".iubenda-cs-accept-btn, #iubenda-cs-accept-btn"))
            )
            cookie_btn.click()
            time.sleep(1)
        except Exception:
            driver.execute_script("""
                var iub = document.getElementById('iubenda-cs-banner');
                if(iub) iub.remove();
                var backdrops = document.querySelectorAll('.modal-backdrop, .iubenda-cs-overlay');
                backdrops.forEach(b => b.remove());
            """)

        # 2. Compila i campi tramite JS injection per prevenire ElementNotInteractableException
        username_input = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        password_input = wait.until(EC.presence_of_element_located((By.NAME, "password")))

        driver.execute_script("""
            arguments[0].focus();
            arguments[0].value = arguments[1];
            arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
        """, username_input, FANTACALCIO_USERNAME)

        driver.execute_script("""
            arguments[0].focus();
            arguments[0].value = arguments[1];
            arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
        """, password_input, FANTACALCIO_PASSWORD)

        time.sleep(1)

        # 3. Invio form
        try:
            submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], form input[type='submit']")
            driver.execute_script("arguments[0].click();", submit_btn)
        except Exception:
            password_input.send_keys(Keys.RETURN)

        time.sleep(4)

        # 4. Navigazione alla pagina quotazioni e download file Excel
        driver.get("https://www.fantacalcio.it/quotazioni-fantacalcio")
        time.sleep(3)

        download_link = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.download-players-price-serie-a, a[href*='Quotazioni_Fantacalcio_Ruolo_Mantra.xlsx'], a[href*='Quotazioni_Fantacalcio']"))
        )
        href = download_link.get_attribute("href")

        if not href.startswith("http"):
            href = f"https://www.fantacalcio.it{href}"

        cookies = driver.get_cookies()
        session = requests.Session()
        for c in cookies:
            session.cookies.set(c["name"], c["value"])

        response = session.get(href, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        with open(TARGET_FILE, "wb") as f:
            f.write(response.content)

    finally:
        driver.quit()

    df = pd.read_excel(TARGET_FILE, engine="openpyxl", header=1)
    return df


# === ETL PROCESS ===
if __name__ == '__main__':
    print("📥 Estrazione e trasformazione dati in corso...")

    # 1️⃣ EXTRACT
    fc = scarica_listone()
    print(f"✅ Listone Fantacalcio scaricato ({len(fc)} record)")

    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        dbname=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    sb = pd.read_sql(f"SELECT * FROM {SUPABASE_TABLE};", conn)
    print(f"✅ Tabella Supabase scaricata ({len(sb)} record)")

    # 2️⃣ TRANSFORM
    fc['priorita'] = 1
    sb['priorita'] = 0
    fc.rename(columns={'Nome': 'nome'}, inplace=True)
    new_sb = pd.concat([sb[['id', 'nome', 'priorita']], fc[['nome', 'priorita']]])
    new_sb.sort_values(by=['priorita'], inplace=True, ascending=False)
    new_sb.drop_duplicates(subset=['nome'], inplace=True)
    new_sb = new_sb.merge(sb, on='nome', how='left')
    
    if 'priorita_y' in new_sb.columns:
        new_sb = new_sb.drop('priorita_y', axis=1)
    if 'priorita_x' in new_sb.columns:
        new_sb.rename(columns={'priorita_x': 'priorita'}, inplace=True)
    new_sb.reset_index(drop=True, inplace=True)

    for col in ['id_x', 'id_y']:
        if col in new_sb.columns:
            new_sb = new_sb.drop(col, axis=1)

    merge_cols = [c for c in ['RM', 'Squadra', 'Qt.A M'] if c in fc.columns]
    if merge_cols:
        new_sb = new_sb.merge(fc[['nome'] + merge_cols], on='nome', how='left', suffixes=('_sb', '_fc'))
        if 'RM' in merge_cols:
            new_sb['ruolo'] = new_sb['RM']
        if 'Squadra' in merge_cols:
            new_sb['club'] = new_sb['Squadra']
        if 'Qt.A M' in merge_cols:
            if 'quot_att_mantra' in new_sb.columns:
                new_sb['quot_att_mantra'] = new_sb['quot_att_mantra'].where(
                    new_sb['Qt.A M'].isna(),
                    new_sb['Qt.A M']
                )
            else:
                new_sb['quot_att_mantra'] = new_sb['Qt.A M']
        for c in merge_cols:
            if c in new_sb.columns:
                new_sb = new_sb.drop(c, axis=1)

    for col, default in [
        ('squadra_att', 'Svincolato'),
        ('detentore_cartellino', 'Svincolato'),
        ('tipo_contratto', 'Svincolato'),
    ]:
        if col in new_sb.columns:
            new_sb[col] = new_sb[col].fillna(default)

    if 'costo' in new_sb.columns:
        new_sb['costo'] = new_sb['costo'].fillna(0)
    
    if 'quot_att_mantra' in new_sb.columns:
        new_sb['quot_att_mantra'] = pd.to_numeric(new_sb['quot_att_mantra'], errors='coerce')

    if 'ruolo' in new_sb.columns:
        new_sb['ruolo'] = new_sb['ruolo'].astype(str).str.replace('{', '').str.replace('}', '')

    print("✅ Trasformazione completata!")

    # === OUTPUT LOCALE ===
    output_path = os.path.join(os.getcwd(), "output_new_sb.xlsx")
    new_sb.to_excel(output_path, index=False)
    print(f"📁 File salvato localmente in: {output_path}")

    # 3️⃣ LOAD SU SUPABASE
    print("⬆️ Caricamento su Supabase in corso...")
    cur = conn.cursor()

    df = new_sb.copy()
    df = df.map(lambda x: None if x is None or str(x).strip() == "" else x)

    for _, row in df.iterrows():
        valore = row.get("ruolo")

        if not valore or pd.isna(valore):
            ruoli = None
        else:
            valore = (
                str(valore)
                .replace("{", "")
                .replace("}", "")
                .replace(";", ",")
                .replace("\n", ",")
                .replace(" ", "")
            )
            ruoli = [v for v in valore.split(",") if v]

        update_fields = []
        update_values = []
        
        if row.get("squadra_att") is not None and not pd.isna(row.get("squadra_att")):
            update_fields.append("squadra_att = %s")
            update_values.append(row.get("squadra_att"))
        
        if row.get("detentore_cartellino") is not None and not pd.isna(row.get("detentore_cartellino")):
            update_fields.append("detentore_cartellino = %s")
            update_values.append(row.get("detentore_cartellino"))
        
        if row.get("club") is not None and not pd.isna(row.get("club")):
            update_fields.append("club = %s")
            update_values.append(row.get("club"))
        
        if row.get("quot_att_mantra") is not None and not pd.isna(row.get("quot_att_mantra")):
            update_fields.append("quot_att_mantra = %s")
            update_values.append(row.get("quot_att_mantra"))
        
        if row.get("tipo_contratto") is not None and not pd.isna(row.get("tipo_contratto")):
            update_fields.append("tipo_contratto = %s")
            update_values.append(row.get("tipo_contratto"))
        
        if ruoli is not None:
            update_fields.append("ruolo = %s::ruolo_mantra[]")
            update_values.append(ruoli)
        
        if row.get("costo") is not None and not pd.isna(row.get("costo")):
            update_fields.append("costo = %s")
            update_values.append(row.get("costo"))
        
        if row.get("priorita") is not None and not pd.isna(row.get("priorita")):
            update_fields.append("priorita = %s")
            update_values.append(row.get("priorita"))
        
        if update_fields:
            update_query = "UPDATE giocatore SET " + ", ".join(update_fields) + " WHERE nome = %s;"
            update_values.append(row.get("nome"))
            cur.execute(update_query, update_values)
        
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO giocatore (
                    nome,
                    squadra_att,
                    detentore_cartellino,
                    club,
                    quot_att_mantra,
                    tipo_contratto,
                    ruolo,
                    costo,
                    priorita
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::ruolo_mantra[], %s, %s);
                """,
                (
                    row.get("nome"),
                    row.get("squadra_att"),
                    row.get("detentore_cartellino"),
                    row.get("club"),
                    row.get("quot_att_mantra"),
                    row.get("tipo_contratto"),
                    ruoli,
                    row.get("costo"),
                    row.get("priorita"),
                )
            )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Dati reinseriti con successo su Supabase")
    print(f"Totale giocatori caricati: {len(new_sb)}")

    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON non configurato nei secret.")
        
    gc = gspread.service_account(GOOGLE_CREDENTIALS_PATH)
    
    rename_mapping = {
        "nome": "Calciatore",
        "ruolo": "Ruolo",
        "club": "CSA",
        "detentore_cartellino": "Detentore Cartellino",
        "squadra_att": "Squadra Attuale",
        "costo": "Costo",
        "tipo_contratto": "Tipo Contratto",
        "quot_att_mantra": "Quotazione Attuale",
        "id": "ID Calciatore"
    }
    
    spreadsheet = gc.open("Test")
    worksheet = spreadsheet.worksheet("Listone")
    
    df.rename(columns=rename_mapping, inplace=True)
    df['Ruolo'] = df['Ruolo'].astype(str).str.replace('{', '').str.replace('}', '')
    df.drop(["priorita"], axis=1, inplace=True, errors='ignore')
    df['ID Calciatore'] = 1
    
    worksheet.clear()
    set_with_dataframe(worksheet, df)
    print("✅ Listone aggiornato nel Google Sheet.")

    # === Aggiorna crediti nel foglio Google Sheet ===
    print("⬆️ Aggiornamento crediti squadre in Google Sheet...")
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        dbname=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    sbc = pd.read_sql(f"SELECT * FROM {SUPABASE_TABLE_CREDITI};", conn)
    sbc = sbc[['nome', 'crediti']]
    sbc.rename(columns={'nome': 'Squadra', 'crediti': 'Crediti'}, inplace=True)    
    conn.close()
    worksheet_crediti = spreadsheet.worksheet("Nuova_Crediti")    
    worksheet_crediti.clear()
    set_with_dataframe(worksheet_crediti, sbc)
    print("✅ Crediti squadre aggiornati nel Google Sheet.")

    # === Aggiorna movimenti mercato nel foglio Google Sheet ===
    print("⬆️ Aggiornamento movimenti mercato in Google Sheet...")
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        dbname=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    sbm = pd.read_sql(f"SELECT * FROM {SUPABASE_TABLE_MOVIMENTI};", conn)
    sbm = sbm[['data', 'evento', 'stagione']]
    sbm.rename(columns={'data': 'Data', 'evento': 'Evento', 'stagione': 'Stagione'}, inplace=True)    
    conn.close()
    worksheet_movimenti = spreadsheet.worksheet("Mercato")    
    worksheet_movimenti.clear()
    set_with_dataframe(worksheet_movimenti, sbm)
    print("✅ Movimenti mercato aggiornati nel Google Sheet.")
    
    # === Aggiorna tabella aste in Google Sheet ===
    print("⬆️ Aggiornamento tabella aste in Google Sheet...")
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        dbname=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    aste = pd.read_sql(f"""
        SELECT a.*, g.nome as nome_giocatore
        FROM {SUPABASE_TABLE_ASTE} a
        LEFT JOIN giocatore g ON a.giocatore = g.id;
    """, conn)
    conn.close()
    worksheet_aste = spreadsheet.worksheet("Durata_Aste")    
    worksheet_aste.clear()
    set_with_dataframe(worksheet_aste, aste)
    print("✅ Durata_Aste aggiornata nel Google Sheet.")
    
    print("=== ETL completato con successo ===")
