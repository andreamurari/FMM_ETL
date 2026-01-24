import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Conn string dirette (sovrascrivibili via env). Metti qui i DSN completi.
SRC_DSN = os.environ.get("SUPABASE_PASSWORD_PROD")
DST_DSN = os.environ.get("SUPABASE_PASSWORD_DEV")

if "<dest-connection-string>" in DST_DSN:
    raise RuntimeError("Imposta DST_SUPABASE_DSN o sostituisci il DSN di destinazione nel file.")

BATCH_SIZE = 1000

# Ordine più sicuro rispetto alle FK più comuni (es. asta/ scambio puntano a giocatore/squadra).
TABLES = [
    "stadio",
    "giocatore",
    "squadra",
    "movimenti_squadra",
    "admin",
    "richiesta_modifica_contratto",
    "asta",
    "prestito",
    "scambio",
    "sessions",
]


def get_pk_columns(conn, table):
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        ORDER BY a.attnum;
    """
    with conn.cursor() as cur:
        cur.execute(query, (table,))
        return [row[0] for row in cur.fetchall()]


def fetch_rows(cur, table, offset, limit):
    query = sql.SQL("SELECT * FROM {} OFFSET %s LIMIT %s").format(sql.Identifier(table))
    cur.execute(query, (offset, limit))
    cols = [desc.name for desc in cur.description]
    return cols, cur.fetchall()


def upsert_rows(cur, table, cols, rows, pk_cols):
    cols_list = ",".join(f'"{c}"' for c in cols)
    placeholders = "(" + ",".join(["%s"] * len(cols)) + ")"

    if pk_cols:
        conflict_cols = "(" + ",".join(f'"{c}"' for c in pk_cols) + ")"
        set_clause = ",".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in pk_cols)
        if set_clause:
            insert_sql = (
                f'INSERT INTO "{table}" ({cols_list}) VALUES %s '
                f"ON CONFLICT {conflict_cols} DO UPDATE SET {set_clause}"
            )
        else:
            insert_sql = (
                f'INSERT INTO "{table}" ({cols_list}) VALUES %s '
                "ON CONFLICT " + conflict_cols + " DO NOTHING"
            )
    else:
        insert_sql = f'INSERT INTO "{table}" ({cols_list}) VALUES %s'

    execute_values(cur, insert_sql, rows, template=placeholders, page_size=BATCH_SIZE)


def truncate_tables(dst_conn, tables):
    """Truncate tutte le tabelle nel DB di destinazione."""
    with dst_conn.cursor() as cur:
        for table in reversed(tables):  # Ordine inverso per gestire le FK
            cur.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(sql.Identifier(table)))
            dst_conn.commit()
            print(f"{table}: truncated")


def copy_table(src_conn, dst_conn, table, batch_size=BATCH_SIZE):
    pk_cols = get_pk_columns(dst_conn, table)
    with src_conn.cursor() as src_cur, dst_conn.cursor() as dst_cur:
        offset = 0
        while True:
            cols, rows = fetch_rows(src_cur, table, offset, batch_size)
            if not rows:
                break
            upsert_rows(dst_cur, table, cols, rows, pk_cols)
            dst_conn.commit()
            offset += len(rows)
            print(f"{table}: copiati {len(rows)} record (tot {offset})")


def main():
    with psycopg2.connect(SRC_DSN) as src_conn, psycopg2.connect(DST_DSN) as dst_conn:
        with dst_conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'replica';")
        try:
            print("=== Truncating destination tables ===")
            truncate_tables(dst_conn, TABLES)
            print("\n=== Copying data from source ===")
            for table in TABLES:
                copy_table(src_conn, dst_conn, table)
        finally:
            with dst_conn.cursor() as cur:
                cur.execute("SET session_replication_role = 'origin';")
            dst_conn.commit()


if __name__ == "__main__":
    main()