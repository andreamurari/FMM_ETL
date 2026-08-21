"""
ETL: Listone Fantacalcio -> Supabase -> Google Sheets

Flusso:
  1. Scarica il listone giocatori da fantacalcio.it (Selenium + requests)
  2. Legge la tabella `giocatore` da Supabase
  3. Unisce/aggiorna i dati (ruolo, club, quotazione dal listone)
  4. Scrive il risultato in un Excel locale
  5. Aggiorna (upsert) i record in Supabase
  6. Pubblica listone, crediti, movimenti mercato e durata aste su Google Sheets
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
from typing import Optional

import gspread
import pandas as pd
import psycopg2
import requests
from gspread_dataframe import set_with_dataframe
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ======================================================================
# CONFIGURAZIONE
# ======================================================================

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

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDENTIALS_PATH = os.path.join(os.getcwd(), "google_credentials.json")
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "Test")

CHROME_BIN = os.environ.get("CHROME_BIN")
CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
TARGET_FILE = os.path.join(DOWNLOAD_DIR, "listone.xlsx")

LOG_FILE = os.path.join(os.getcwd(), "log.txt")
DEBUG_SCREENSHOT = os.path.join(os.getcwd(), "debug_screenshot.png")
DEBUG_HTML = os.path.join(os.getcwd(), "debug_page.html")

RENAME_MAPPING = {
    "nome": "Calciatore",
    "ruolo": "Ruolo",
    "club": "CSA",
    "detentore_cartellino": "Detentore Cartellino",
    "squadra_att": "Squadra Attuale",
    "costo": "Costo",
    "tipo_contratto": "Tipo Contratto",
    "quot_att_mantra": "Quotazione Attuale",
    "id": "ID Calciatore",
}

# ======================================================================
# LOGGING (su file + stdout, cosi' compare anche nei log della Action)
# ======================================================================

logger = logging.getLogger("fc_to_sb_to_gs_ETL")
logger.setLevel(logging.INFO)
_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)

_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setFormatter(_formatter)
logger.addHandler(_stream_handler)


class ConfigError(RuntimeError):
    """Errore di configurazione: manca un secret o una variabile richiesta."""


def check_config() -> None:
    if not SUPABASE_PASSWORD:
        raise ConfigError("Manca la variabile d'ambiente SUPABASE_PASSWORD (impostala come GitHub Secret).")
    if not FANTACALCIO_PASSWORD:
        raise ConfigError("Manca la variabile d'ambiente FANTACALCIO_PASSWORD (impostala come GitHub Secret).")
    if not GOOGLE_CREDENTIALS_JSON:
        raise ConfigError("Manca la variabile d'ambiente GOOGLE_CREDENTIALS_JSON (impostala come GitHub Secret).")


def write_google_credentials() -> None:
    with open(GOOGLE_CREDENTIALS_PATH, "w", encoding="utf-8") as f:
        f.write(GOOGLE_CREDENTIALS_JSON)


# ======================================================================
# STEP 1 - DOWNLOAD LISTONE (Selenium)
# ======================================================================

def _build_chrome_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    # Riduce le probabilita' che il sito rilevi l'automazione e mostri un layout diverso
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    chrome_bin = CHROME_BIN or shutil.which("google-chrome") or shutil.which("chromium") or shutil.which("chromium-browser")
    if chrome_bin:
        options.binary_location = chrome_bin

    # Selenium Manager (integrato da Selenium 4.6+) risolve automaticamente il chromedriver
    # corretto per la versione di Chrome installata. Se e' stato passato un path esplicito
    # via env (CHROMEDRIVER_PATH), lo si usa comunque, per compatibilita'.
    service = Service(executable_path=CHROMEDRIVER_PATH) if CHROMEDRIVER_PATH else Service()

    return webdriver.Chrome(service=service, options=options)


def _accept_cookie_banner_if_present(driver: webdriver.Chrome) -> None:
    """Chiude eventuali banner cookie/consenso che potrebbero bloccare i click sul form."""
    selectors = [
        (By.ID, "onetrust-accept-btn-handler"),
        (By.XPATH, "//button[contains(translate(., 'ACEPT', 'acept'), 'accett')]"),
        (By.XPATH, "//button[contains(translate(., 'ACONSENTO', 'aconsento'), 'consent')]"),
    ]
    for by, selector in selectors:
        try:
            btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((by, selector)))
            btn.click()
            logger.info("Banner cookie chiuso (%s).", selector)
            return
        except Exception:
            continue


def _save_debug_artifacts(driver: webdriver.Chrome) -> None:
    try:
        driver.save_screenshot(DEBUG_SCREENSHOT)
        with open(DEBUG_HTML, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        logger.info("Salvati artefatti di debug: %s, %s", DEBUG_SCREENSHOT, DEBUG_HTML)
    except Exception as exc:
        logger.warning("Impossibile salvare gli artefatti di debug: %s", exc)


def scarica_listone() -> pd.DataFrame:
    """Scarica il listone Fantacalcio tramite Selenium e Requests."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    driver = _build_chrome_driver()

    try:
        driver.get("https://www.fantacalcio.it/login")
        _accept_cookie_banner_if_present(driver)

        username_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.NAME, "username"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", username_input)
        username_input.click()
        username_input.send_keys(FANTACALCIO_USERNAME)

        password_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.NAME, "password"))
        )
        password_input.send_keys(FANTACALCIO_PASSWORD)
        password_input.send_keys(Keys.RETURN)

        WebDriverWait(driver, 20).until(EC.url_contains("fantacalcio.it"))
        driver.get("https://www.fantacalcio.it/quotazioni-fantacalcio")

        download_link = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.download-players-price-serie-a"))
        )
        href = download_link.get_attribute("href")

        session = requests.Session()
        for cookie in driver.get_cookies():
            session.cookies.set(cookie["name"], cookie["value"])

        response = session.get(href)
        response.raise_for_status()
        with open(TARGET_FILE, "wb") as f:
            f.write(response.content)

    except Exception:
        logger.error("Errore durante il download del listone. Salvo screenshot/HTML per debug.")
        _save_debug_artifacts(driver)
        raise
    finally:
        driver.quit()

    return pd.read_excel(TARGET_FILE, engine="openpyxl", header=1)


# ======================================================================
# STEP 2 - DB HELPERS
# ======================================================================

def db_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        dbname=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
    )


def load_table(conn, table_name: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {table_name};", conn)


# ======================================================================
# STEP 3 - TRANSFORM
# ======================================================================

def transform(fc: pd.DataFrame, sb: pd.DataFrame) -> pd.DataFrame:
    fc = fc.copy()
    sb = sb.copy()

    fc["priorita"] = 1
    sb["priorita"] = 0
    fc.rename(columns={"Nome": "nome"}, inplace=True)

    new_sb = pd.concat([sb[["id", "nome", "priorita"]], fc[["nome", "priorita"]]])
    new_sb.sort_values(by=["priorita"], inplace=True, ascending=False)
    new_sb.drop_duplicates(subset=["nome"], inplace=True)
    new_sb = new_sb.merge(sb, on="nome", how="left")

    if "priorita_y" in new_sb.columns:
        new_sb = new_sb.drop("priorita_y", axis=1)
    if "priorita_x" in new_sb.columns:
        new_sb.rename(columns={"priorita_x": "priorita"}, inplace=True)
    new_sb.reset_index(drop=True, inplace=True)

    for col in ["id_x", "id_y"]:
        if col in new_sb.columns:
            new_sb = new_sb.drop(col, axis=1)

    merge_cols = [c for c in ["RM", "Squadra", "Qt.A M"] if c in fc.columns]
    if merge_cols:
        new_sb = new_sb.merge(fc[["nome"] + merge_cols], on="nome", how="left", suffixes=("_sb", "_fc"))

        if "RM" in merge_cols:
            new_sb["ruolo"] = new_sb["RM"]
        if "Squadra" in merge_cols:
            new_sb["club"] = new_sb["Squadra"]
        if "Qt.A M" in merge_cols:
            if "quot_att_mantra" in new_sb.columns:
                new_sb["quot_att_mantra"] = new_sb["quot_att_mantra"].where(
                    new_sb["Qt.A M"].isna(), new_sb["Qt.A M"]
                )
            else:
                new_sb["quot_att_mantra"] = new_sb["Qt.A M"]

        for c in merge_cols:
            if c in new_sb.columns:
                new_sb = new_sb.drop(c, axis=1)

    for col, default in [
        ("squadra_att", "Svincolato"),
        ("detentore_cartellino", "Svincolato"),
        ("tipo_contratto", "Svincolato"),
    ]:
        if col in new_sb.columns:
            new_sb[col] = new_sb[col].fillna(default)

    if "costo" in new_sb.columns:
        new_sb["costo"] = new_sb["costo"].fillna(0)

    if "quot_att_mantra" in new_sb.columns:
        new_sb["quot_att_mantra"] = pd.to_numeric(new_sb["quot_att_mantra"], errors="coerce")

    if "ruolo" in new_sb.columns:
        new_sb["ruolo"] = new_sb["ruolo"].astype(str).str.replace("{", "").str.replace("}", "")

    return new_sb


def clean_for_db(new_sb: pd.DataFrame) -> pd.DataFrame:
    """Converte le stringhe vuote in None (-> NULL in Postgres)."""
    return new_sb.map(lambda x: None if x is None or str(x).strip() == "" else x)


def _parse_ruoli(raw_value) -> Optional[list]:
    if raw_value is None or pd.isna(raw_value):
        return None
    value = (
        str(raw_value)
        .replace("{", "")
        .replace("}", "")
        .replace(";", ",")
        .replace("\n", ",")
        .replace(" ", "")
    )
    ruoli = [v for v in value.split(",") if v]
    return ruoli or None


def upsert_giocatore(cur, row: pd.Series) -> None:
    """Aggiorna il giocatore se esiste (per nome), altrimenti lo inserisce."""
    ruoli = _parse_ruoli(row.get("ruolo"))

    field_map = [
        ("squadra_att", "squadra_att = %s"),
        ("detentore_cartellino", "detentore_cartellino = %s"),
        ("club", "club = %s"),
        ("quot_att_mantra", "quot_att_mantra = %s"),
        ("tipo_contratto", "tipo_contratto = %s"),
        ("costo", "costo = %s"),
        ("priorita", "priorita = %s"),
    ]

    update_fields, update_values = [], []
    for col, clause in field_map:
        value = row.get(col)
        if value is not None and not pd.isna(value):
            update_fields.append(clause)
            update_values.append(value)

    if ruoli is not None:
        update_fields.append("ruolo = %s::ruolo_mantra[]")
        update_values.append(ruoli)

    updated = False
    if update_fields:
        query = "UPDATE giocatore SET " + ", ".join(update_fields) + " WHERE nome = %s;"
        cur.execute(query, update_values + [row.get("nome")])
        updated = cur.rowcount > 0

    if not updated:
        cur.execute(
            """
            INSERT INTO giocatore (
                nome, squadra_att, detentore_cartellino, club,
                quot_att_mantra, tipo_contratto, ruolo, costo, priorita
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
            ),
        )


def load_to_supabase(conn, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    errori = 0
    for _, row in df.iterrows():
        try:
            upsert_giocatore(cur, row)
        except Exception as exc:
            errori += 1
            logger.warning("Errore su upsert giocatore '%s': %s", row.get("nome"), exc)
            conn.rollback()
        else:
            conn.commit()
    cur.close()
    if errori:
        logger.warning("Upsert completato con %d riga/e in errore su %d totali.", errori, len(df))


# ======================================================================
# STEP 4 - GOOGLE SHEETS
# ======================================================================

def get_spreadsheet() -> gspread.Spreadsheet:
    gc = gspread.service_account(GOOGLE_CREDENTIALS_PATH)
    return gc.open(GOOGLE_SHEET_NAME)


def publish_listone(spreadsheet: gspread.Spreadsheet, df: pd.DataFrame) -> None:
    df = df.rename(columns=RENAME_MAPPING)
    if "Ruolo" in df.columns:
        df["Ruolo"] = df["Ruolo"].astype(str).str.replace("{", "").str.replace("}", "")
    df = df.drop(columns=["priorita"], errors="ignore")
    df["ID Calciatore"] = 1  # comportamento storico mantenuto dal foglio originale

    worksheet = spreadsheet.worksheet("Listone")
    worksheet.clear()
    set_with_dataframe(worksheet, df)


def publish_crediti(spreadsheet: gspread.Spreadsheet, conn) -> None:
    sbc = load_table(conn, SUPABASE_TABLE_CREDITI)[["nome", "crediti"]]
    sbc = sbc.rename(columns={"nome": "Squadra", "crediti": "Crediti"})
    worksheet = spreadsheet.worksheet("Nuova_Crediti")
    worksheet.clear()
    set_with_dataframe(worksheet, sbc)


def publish_movimenti(spreadsheet: gspread.Spreadsheet, conn) -> None:
    sbm = load_table(conn, SUPABASE_TABLE_MOVIMENTI)[["data", "evento", "stagione"]]
    sbm = sbm.rename(columns={"data": "Data", "evento": "Evento", "stagione": "Stagione"})
    worksheet = spreadsheet.worksheet("Mercato")
    worksheet.clear()
    set_with_dataframe(worksheet, sbm)


def publish_aste(spreadsheet: gspread.Spreadsheet, conn) -> None:
    aste = pd.read_sql(
        f"""
        SELECT a.*, g.nome AS nome_giocatore
        FROM {SUPABASE_TABLE_ASTE} a
        LEFT JOIN giocatore g ON a.giocatore = g.id;
        """,
        conn,
    )
    worksheet = spreadsheet.worksheet("Durata_Aste")
    worksheet.clear()
    set_with_dataframe(worksheet, aste)


# ======================================================================
# MAIN
# ======================================================================

def main() -> None:
    check_config()
    write_google_credentials()

    logger.info("Estrazione e trasformazione dati in corso...")

    fc = scarica_listone()
    logger.info("Listone Fantacalcio scaricato (%d record)", len(fc))

    conn = db_connect()
    try:
        sb = load_table(conn, SUPABASE_TABLE)
        logger.info("Tabella Supabase scaricata (%d record)", len(sb))

        new_sb = transform(fc, sb)
        logger.info("Trasformazione completata!")

        output_path = os.path.join(os.getcwd(), "output_new_sb.xlsx")
        new_sb.to_excel(output_path, index=False)
        logger.info("File salvato localmente in: %s", output_path)

        df = clean_for_db(new_sb)

        logger.info("Caricamento su Supabase in corso...")
        load_to_supabase(conn, df)
        logger.info("Dati reinseriti con successo. Totale giocatori: %d", len(new_sb))

        spreadsheet = get_spreadsheet()

        logger.info("Aggiornamento listone in Google Sheet...")
        publish_listone(spreadsheet, df)
        logger.info("Listone aggiornato nel Google Sheet.")

        logger.info("Aggiornamento crediti squadre in Google Sheet...")
        publish_crediti(spreadsheet, conn)
        logger.info("Crediti squadre aggiornati nel Google Sheet.")

        logger.info("Aggiornamento movimenti mercato in Google Sheet...")
        publish_movimenti(spreadsheet, conn)
        logger.info("Movimenti mercato aggiornati nel Google Sheet.")

        logger.info("Aggiornamento tabella aste in Google Sheet...")
        publish_aste(spreadsheet, conn)
        logger.info("Durata_Aste aggiornata nel Google Sheet.")

    finally:
        conn.close()

    logger.info("=== ETL completato con successo ===")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("ETL fallito con un errore non gestito.")
        raise
