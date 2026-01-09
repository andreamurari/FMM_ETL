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

FANTACALCIO_USERNAME = os.environ.get("FANTACALCIO_USERNAME", "mura88")
FANTACALCIO_PASSWORD = os.environ.get("FANTACALCIO_PASSWORD")  # <- SECRET

# Optional: JSON content for Google credentials (if needed by your pipeline)
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")  # <- SECRET (raw JSON string)
GOOGLE_CREDENTIALS_PATH = os.path.join(os.getcwd(), "google_credentials.json")
if GOOGLE_CREDENTIALS_JSON:
    with open(GOOGLE_CREDENTIALS_PATH, "w", encoding="utf-8") as f:
        f.write(GOOGLE_CREDENTIALS_JSON)
    
# Controlli minimi sui secrets
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

# === FUNZIONE DOWNLOAD LISTONE FANTACALCIO ===
def scarica_listone():
    """Scarica il listone Fantacalcio tramite Selenium e Requests"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    # Se Chromium è installato in CI, proviamo a usarlo
    chromium_path = shutil.which("chromium") or shutil.which("chromium-browser") or shutil.which("google-chrome")
    if chromium_path:
        options.binary_location = chromium_path

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get("https://www.fantacalcio.it/login")

        username_input = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )
        password_input = driver.find_element(By.NAME, "password")

        username_input.send_keys(FANTACALCIO_USERNAME)
        password_input.send_keys(FANTACALCIO_PASSWORD)
        password_input.send_keys(Keys.RETURN)

        WebDriverWait(driver, 20).until(EC.url_contains("fantacalcio.it"))
        driver.get("https://www.fantacalcio.it/quotazioni-fantacalcio")

        download_link = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.download-players-price-serie-a"))
        )
        href = download_link.get_attribute("href")

        cookies = driver.get_cookies()
        session = requests.Session()
        for c in cookies:
            session.cookies.set(c["name"], c["value"])

        response = session.get(href)
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
    # Se la tabella sb non aveva priorita_y, proteggiamo
    if 'priorita_y' in new_sb.columns:
        new_sb = new_sb.drop('priorita_y', axis=1)
    if 'priorita_x' in new_sb.columns:
        new_sb.rename(columns={'priorita_x': 'priorita'}, inplace=True)
    new_sb.reset_index(drop=True, inplace=True)

    # Cerca colonne id_x/id_y e rimuovile se presenti
    for col in ['id_x', 'id_y']:
        if col in new_sb.columns:
            new_sb = new_sb.drop(col, axis=1)

    # Merge di colonne dal fc (se presenti)
    merge_cols = [c for c in ['RM', 'Squadra', 'Qt.A M'] if c in fc.columns]
    if merge_cols:
        new_sb = new_sb.merge(fc[['nome'] + merge_cols], on='nome', how='left', suffixes=('_sb', '_fc'))
        if 'RM' in merge_cols:
            new_sb['ruolo'] = new_sb['ruolo'].fillna(new_sb['RM'])
        if 'Squadra' in merge_cols:
            new_sb['club'] = new_sb['club'].fillna(new_sb['Squadra'])
        if 'Qt.A M' in merge_cols:
            new_sb['quot_att_mantra'] = new_sb['quot_att_mantra'].fillna(new_sb['Qt.A M'])
        for c in merge_cols:
            if c in new_sb.columns:
                new_sb = new_sb.drop(c, axis=1)

    # Pulizia valori mancanti / default
    for col, default in [
        ('squadra_att', 'Svincolato'),
        ('detentore_cartellino', 'Svincolato'),
        ('tipo_contratto', 'Svincolato'),
    ]:
        if col in new_sb.columns:
            new_sb[col] = new_sb[col].fillna(default)

    if 'costo' in new_sb.columns:
        new_sb['costo'] = new_sb['costo'].fillna(0)

    if 'ruolo' in new_sb.columns:
        new_sb['ruolo'] = new_sb['ruolo'].astype(str).str.replace('{', '').str.replace('}', '')

    print("✅ Trasformazione completata!")

    # === OUTPUT LOCALE ===
    #output_path = os.path.join(os.getcwd(), "output_new_sb.xlsx")
    #new_sb.to_excel(output_path, index=False)
    #print(f"📁 File salvato localmente in: {output_path}")

    # 3️⃣ LOAD SU SUPABASE
    print("⬆️ Caricamento su Supabase in corso...")
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE giocatore RESTART IDENTITY CASCADE;")
    print("✅ Tabella giocatore svuotata.")

    # 🔧 Converte stringhe vuote in None (NULL in Postgres)
    df = new_sb.copy()
    df = df.applymap(lambda x: None if x is None or str(x).strip() == "" else x)

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
    print("✅ Dati reinseriti con successo: ")
    print(f"Totale giocatori caricati: {len(new_sb)}")

    if GOOGLE_CREDENTIALS_PATH:
        gc = gspread.service_account(GOOGLE_CREDENTIALS_PATH)
    else:
    # Aggiungi un messaggio di errore nel caso manchi la variabile d'ambiente
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON not found in environment or path not created.")
        
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
    df.drop(["priorita"], axis=1, inplace=True)
    df['ID Calciatore'] = len(df['Calciatore'])*[1]
    print("✅ Trasformazione completata con successo!")
    
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
    sbc = sbc['nome', 'crediti']
    sbc.rename(columns={'nome': 'Squadra', 'crediti': 'Crediti'}, inplace=True)    
    worksheet_crediti = spreadsheet.worksheet("Nuova_Crediti")    
    worksheet_crediti.clear()
    set_with_dataframe(worksheet_crediti, sbc)
    print("✅ Crediti squadre aggiornati nel Google Sheet.")

    print("🎉 ETL completato con successo!")







