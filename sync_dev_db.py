import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Conn string dirette (sovrascrivibili via env). Metti qui i DSN completi.
SRC_DSN = os.environ.get("SUPABASE_PASSWORD_PROD")
DST_DSN = os.environ.get("SUPABASE_PASSWORD_DEV")

if not SRC_DSN or not DST_DSN:
    raise RuntimeError(
        "Imposta le variabili d'ambiente SUPABASE_PASSWORD_PROD e SUPABASE_PASSWORD_DEV "
        "con i DSN completi di prod e dev."
    )

if "<dest-connection-string>" in DST_DSN:
    raise RuntimeError("Imposta SUPABASE_PASSWORD_DEV o sostituisci il DSN di destinazione nel file.")

BATCH_SIZE = 1000

# Tabelle da NON sincronizzare (oltre allo schema: solo 'public' viene considerato).
# Override via env: EXCLUDE_TABLES="tab1,tab2"
EXCLUDE_TABLES = {
    t.strip()
    for t in os.environ.get("EXCLUDE_TABLES", "").split(",")
    if t.strip()
}


def get_public_tables(conn):
    """Elenco di tutte le tabelle ordinarie nello schema public."""
    query = """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'r'
        ORDER BY c.relname;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]


def get_fk_edges(conn):
    """Coppie (child, parent): child ha una foreign key che punta a parent."""
    query = """
        SELECT c_child.relname AS child, c_parent.relname AS parent
        FROM pg_constraint con
        JOIN pg_class c_child  ON c_child.oid  = con.conrelid
        JOIN pg_class c_parent ON c_parent.oid = con.confrelid
        JOIN pg_namespace n    ON n.oid = con.connamespace
        WHERE con.contype = 'f'
          AND n.nspname = 'public';
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def topo_sort(tables, edges):
    """Ordina le tabelle in modo che ogni parent preceda i suoi child.

    Le self-reference vengono ignorate. Eventuali cicli (FK mutue) vengono
    risolti mettendo le tabelle rimanenti in coda: con
    session_replication_role = 'replica' i vincoli FK sono comunque disattivati
    durante la copia.
    """
    import heapq

    tset = set(tables)
    deps = {t: set() for t in tables}      # child -> parents non ancora inseriti
    children = {t: set() for t in tables}  # parent -> child

    for child, parent in edges:
        if child == parent:
            continue
        if child not in tset or parent not in tset:
            continue
        if parent in deps[child]:
            continue
        deps[child].add(parent)
        children[parent].add(child)

    heap = [t for t in tables if not deps[t]]
    heapq.heapify(heap)
    order = []
    seen = set()

    while heap:
        t = heapq.heappop(heap)
        if t in seen:
            continue
        seen.add(t)
        order.append(t)
        for ch in sorted(children[t]):
            deps[ch].discard(t)
            if not deps[ch] and ch not in seen:
                heapq.heappush(heap, ch)

    leftover = [t for t in tables if t not in seen]
    if leftover:
        print(f"[WARN] Dipendenze cicliche/irrisolte, sincronizzate comunque in coda: {leftover}")
        order.extend(sorted(leftover))

    return order


def resolve_tables(src_conn):
    """Tabelle da sincronizzare, in ordine di insert sicuro rispetto alle FK."""
    tables = [t for t in get_public_tables(src_conn) if t not in EXCLUDE_TABLES]
    edges = get_fk_edges(src_conn)
    ordered = topo_sort(tables, edges)
    print(f"Tabelle da sincronizzare ({len(ordered)}): {ordered}")
    if EXCLUDE_TABLES:
        print(f"Escluse: {sorted(EXCLUDE_TABLES)}")
    return ordered


def get_pk_columns(conn, table):
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        ORDER BY a.attnum;
    """
    with conn.cursor() as cur:
        cur.execute(query, (f'public."{table}"',))
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
    """Truncate delle tabelle nel DB di destinazione (ordine inverso rispetto alle FK)."""
    with dst_conn.cursor() as cur:
        for table in reversed(tables):
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
        tables = resolve_tables(src_conn)

        with dst_conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'replica';")
        try:
            print("=== Truncating destination tables ===")
            truncate_tables(dst_conn, tables)
            print("\n=== Copying data from source ===")
            for table in tables:
                copy_table(src_conn, dst_conn, table)
        finally:
            with dst_conn.cursor() as cur:
                cur.execute("SET session_replication_role = 'origin';")
            dst_conn.commit()


if __name__ == "__main__":
    main()
