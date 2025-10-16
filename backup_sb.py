import psycopg2
import pandas as pd
import os
from datetime import datetime

db_password = os.environ.get("SUPABASE_PASSWORD")

# 🔐 Credenziali Supabase (modifica con le tue)
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

# 🔌 Connessione al database
conn = psycopg2.connect(
    host=SUPABASE_HOST,
    database=SUPABASE_DB,
    user=SUPABASE_USER,
    password=SUPABASE_PASSWORD,
    port=SUPABASE_PORT
)

cur = conn.cursor()

# 📜 Ottieni tutte le tabelle pubbliche
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
""")

tables = [row[0] for row in cur.fetchall()]

print(f"Trovate {len(tables)} tabelle: {tables}")

# 💾 Esporta ogni tabella in CSV
for table in tables:
    df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
    filepath = os.path.join(OUTPUT_DIR, f"{table}.csv")
    df.to_csv(filepath, index=False)
    print(f"✅ Esportata {table} → {filepath}")

# 🔒 Chiudi connessione
cur.close()
conn.close()
print("🎉 Esportazione completata!")
