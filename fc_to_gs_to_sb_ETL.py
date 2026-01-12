import os
import shutil
import time
import sys
import json
import logging
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import numpy as np
import psycopg2
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import warnings
warnings.filterwarnings("ignore")

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log.txt"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ----------------------------
# Config
# ----------------------------
MAX_RETRIES = 5
WAIT_SECONDS = 5

download_dir = os.path.join(os.getcwd(), "downloads")
os.makedirs(download_dir, exist_ok=True)
target_file = os.path.join(download_dir, "listone.xlsx")

# ----------------------------
# Legge credenziali Google da ENV
# ----------------------------
gspread_credentials_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
if not gspread_credentials_json:
    raise RuntimeError("❌ Variabile d'ambiente GOOGLE_CREDENTIALS_JSON non trovata")

gc = gspread.service_account_from_dict(json.loads(gspread_credentials_json))

# ----------------------------
# Funzione download + parsing
# ----------------------------
def scarica_listone():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        options.binary_location = chrome_bin

    chromedriver_path = os.environ.get("CHROMEDRIVER_PATH")
    if chromedriver_path:
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)

    try:
        logger.info("🔹 Apertura pagina login Fantacalcio")
        driver.get("https://www.fantacalcio.it/login")
        username_input = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )
        password_input = driver.find_element(By.NAME, "password")
        FC_password = os.environ.get("FANTACALCIO_PASSWORD")
        logger.info(f"Password trovata? {'SI' if FC_password else 'NO'}")
        username_input.send_keys("mura88")
        password_input.send_keys(FC_password)
        password_input.send_keys(Keys.RETURN)

        WebDriverWait(driver, 20).until(EC.url_contains("fantacalcio.it"))
        driver.get("https://www.fantacalcio.it/quotazioni-fantacalcio")

        download_link = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.download-players-price-serie-a"))
        )
        href = download_link.get_attribute("href")

        logger.info(f"🔹 Scaricando file listone da {href}")
        cookies = driver.get_cookies()
        session = requests.Session()
        for c in cookies:
            session.cookies.set(c['name'], c['value'])

        response = session.get(href)
        with open(target_file, "wb") as f:
            f.write(response.content)

    finally:
        driver.quit()

    # Lettura file
    try:
        df = pd.read_excel(target_file, engine="openpyxl")
        return df
    except Exception:
        with open(target_file, "rb") as f:
            head = f.read(200).decode(errors="ignore")
        if "<html" in head.lower():
            logger.warning("⚠️ File scaricato in formato HTML, uso pd.read_html()")
            return pd.read_html(target_file)[0]
        elif ";" in head or "," in head:
            try:
                logger.warning("⚠️ File scaricato in formato CSV, uso sep=';'")
                return pd.read_csv(target_file, sep=";")
            except:
                logger.warning("⚠️ Uso sep=',' per CSV")
                return pd.read_csv(target_file, sep=",")
        else:
            raise ValueError("❌ Formato file sconosciuto")

# ----------------------------
# Retry loop per scarico listone
# ----------------------------
listone_fantacalcio = None
for attempt in range(1, MAX_RETRIES + 1):
    try:
        logger.info(f"Tentativo {attempt} di scaricare il listone...")
        listone_fantacalcio = scarica_listone()
        logger.info("✅ Listone scaricato e letto con successo.")
        break
    except Exception as e:
        logger.error(f"❌ Errore al tentativo {attempt}: {e}")
        if attempt < MAX_RETRIES:
            logger.info(f"⏳ Riprovo tra {WAIT_SECONDS} secondi...")
            time.sleep(WAIT_SECONDS)
        else:
            raise RuntimeError("❌ Impossibile scaricare il listone dopo vari tentativi.") from e

# ----------------------------
# Google Sheets
# ----------------------------
spreadsheet = gc.open("Test")
worksheet = spreadsheet.worksheet("Appoggio_listone")
worksheet_test = spreadsheet.worksheet("Supabase")
worksheet_listone = spreadsheet.worksheet("Listone")

rows = worksheet.get_all_values()
header_row_index = next(i for i, row in enumerate(rows) if any(cell.strip() for cell in row))
header = rows[header_row_index]
data_rows = rows[header_row_index + 1:]
old_appoggio_listone = pd.DataFrame(data_rows, columns=header).dropna(axis=1, how='all')

logger.info("✅ Estratto 'Appoggio Listone' da Google.")

# ----------------------------
# Creazione nuova tabella
# ----------------------------
listone_fantacalcio.columns = listone_fantacalcio.iloc[0]
listone_fantacalcio = listone_fantacalcio[1:]

old_appoggio_listone['priorita'] = 0
listone_fantacalcio['priorita'] = 1
nuovo_appoggio_listone = pd.concat([old_appoggio_listone, listone_fantacalcio])
nuovo_appoggio_listone.sort_values(by=['priorita'], inplace=True, ascending=False)
nuovo_appoggio_listone.drop_duplicates(subset=['Nome'], inplace=True)
appoggio = nuovo_appoggio_listone

rows = worksheet_listone.get_all_values()
header_row_index = next(i for i, row in enumerate(rows) if any(cell.strip() for cell in row))
header = rows[header_row_index]
data_rows = rows[header_row_index + 1:]
listone = pd.DataFrame(data_rows, columns=header)

new_test = appoggio.merge(listone, left_on="Nome", right_on="Calciatore", how="left")

new_test.loc[:, ['Detentore Cartellino_y', 'Squadra Attuale_y', 'Tipo Contratto_y']] = \
    new_test[['Detentore Cartellino_y', 'Squadra Attuale_y', 'Tipo Contratto_y']].fillna('Svincolato')
new_test.loc[:, ['Detentore Cartellino_y', 'Squadra Attuale_y', 'Tipo Contratto_y']] = \
    new_test[['Detentore Cartellino_y', 'Squadra Attuale_y', 'Tipo Contratto_y']].replace('/', 'Svincolato')
new_test['Costo_y'].replace('', np.nan, inplace=True)
new_test.loc[:, 'Costo_y'] = new_test['Costo_y'].fillna(0).replace('/', 0)


new_test = new_test[['Nome', 'RM', 'Squadra', 'Detentore Cartellino_y', 'Squadra Attuale_y', 'Costo_y', 'Tipo Contratto_y', 'Qt.A M', 'priorita']]
new_test = new_test.rename(columns={
    'RM': 'ruolo',
    'Nome': 'nome',
    'Squadra': 'club',
    'Detentore Cartellino_y': 'detentore_cartellino',
    'Squadra Attuale_y': 'squadra_att',
    'Costo_y': 'costo',
    'Tipo Contratto_y': 'tipo_contratto',
    'Qt.A M': 'quot_att_mantra',
    "priotità": "priotità"
})
logger.info("✅ Modifiche implementate nella nuova tabella.")

# ----------------------------
# Upload su Google Sheets
# ----------------------------
worksheet_test.clear()
set_with_dataframe(worksheet_test, new_test)
worksheet.clear()
set_with_dataframe(worksheet, nuovo_appoggio_listone)
logger.info("✅ Modifiche caricate nel Google Sheet.")

# ----------------------------
# Pulizia downloads
# ----------------------------
shutil.rmtree(download_dir, ignore_errors=True)

# ----------------------------
# Load in Supabase
# ----------------------------
db_password = os.environ.get("SUPABASE_PASSWORD")
if not db_password:
    raise RuntimeError("❌ Variabile d'ambiente SUPABASE_PASSWORD non trovata")

conn = psycopg2.connect(
    host="aws-1-eu-central-1.pooler.supabase.com",
    port=6543,
    dbname="postgres",
    user="postgres.vhowswomnwhbfdpslsep",
    password=db_password
)
cur = conn.cursor()

rows = worksheet_test.get_all_values()
header_row_index = next(i for i, row in enumerate(rows) if any(cell.strip() for cell in row))
header = rows[header_row_index]
data_rows = rows[header_row_index + 1:]
df = pd.DataFrame(data_rows, columns=header)
df = df.map(lambda x: None if x is None or str(x).strip() == "" else x)

for _, row in df.iterrows():
    valore = row.get("ruolo")
    if valore is None:
        ruoli = None
    else:
        valore = valore.replace("\n", ";")
        ruoli = [v.strip() for v in valore.split(";") if v.strip()]
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
    VALUES (%s, %s, %s, %s, %s, %s, %s::ruolo_mantra[], %s, %s)
    ON CONFLICT (nome) DO UPDATE SET
        squadra_att = EXCLUDED.squadra_att,
        detentore_cartellino = EXCLUDED.detentore_cartellino,
        club = EXCLUDED.club,
        quot_att_mantra = EXCLUDED.quot_att_mantra,
        tipo_contratto = EXCLUDED.tipo_contratto,
        ruolo = EXCLUDED.ruolo,
        costo = EXCLUDED.costo,
        priorita = EXCLUDED.priorita;
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
        row.get("priorita")  
    )
)
conn.commit()
cur.close()
conn.close()
logger.info("✅ Dati reinseriti con successo in Supabase.")
