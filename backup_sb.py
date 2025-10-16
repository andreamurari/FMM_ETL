import psycopg2
import pandas as pd
import os
from datetime import datetime

# 🔐 Recupero password da variabile d'ambiente
db_password = os.environ.get("SUPABASE_PASSWORD")

# 🔍 Controllo se la password è presente
print("Password presente?", bool(db_password))
if not db_password:
    print("⚠️ ATTENZIONE: la variabile d'ambiente SUPABASE_PASSWORD non è impostata.")
    print("Puoi impostarla con: setx SUPABASE_PASSWORD \"la_tua_password\" e poi riavviare il terminale.")
    # 👉 Solo per test, puoi decommentare la riga seguente e inserire la password a mano
    # db_password = "INSERISCI_LA_TUA_PASSWORD"

# 🔐 Credenziali Supabase (modifica se necessario)
SUPABASE_HOST = "aws-1-eu-central-1.pooler.supabase.com"
SUPABASE_PORT = 6543
SUPABASE_DB = "postgres"
SUPABASE_USER = "postgres.vhowswomnwhbfdpslsep"
SUPABASE_PASSWORD = db_password

# 📁 Cartella principale di output
BASE_DIR = r"G:\Il mio Drive\MANTRA MANAGERIALE\script_refresh_listone\nuovo_flusso_supabase\backup"

# 📅 Crea sottocartella con data nel formato yyyy_mm_dd
today_str = datetime.now().strftime("%Y_%m_%d")
OUTPUT_DIR = os.path.join(BASE_DIR, today_str)
os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"📂 Le tabelle verranno salvate in: {OUTPUT_DIR}")
print("Percorso assoluto:", os.path.abspath(OUTPUT_DIR))
print("Esiste la cartella?", os.path.exists(OUTPUT_DIR))
print("Contenuto della cartella padre:", os.listdir(os.path.dirname(OUTPUT_DIR)))

# 🔌 Connessione al database
try:
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        database=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        port=SUPABASE_PORT
    )
    cur = conn.cursor()
    print("✅ Connessione a Supabase riuscita!")
except Exception as e:
    print("❌ Errore di connessione a Supabase:", e)
    raise SystemExit

# 📜 Ottieni tutte le tabelle pubbliche
try:
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = [row[0] for row in cur.fetchall()]
    print(f"📋 Tabelle trovate ({len(tables)}): {tables}")
except Exception as e:
    print("❌ Errore durante il recupero delle tabelle:", e)
    cur.close()
    conn.close()
    raise SystemExit

# 💾 Esporta ogni tabella in CSV
for table in tables:
    try:
        print(f"⏳ Esporto {table}...")
        df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
        filepath = os.path.join(OUTPUT_DIR, f"{table}.csv")
        df.to_csv(filepath, index=False)
        print(f"✅ Esportata {table} → {filepath}")
    except Exception as e:
        print(f"❌ Errore su {table}: {e}")

# 🔒 Chiudi connessione
cur.close()
conn.close()
print("🎉 Esportazione completata!")
