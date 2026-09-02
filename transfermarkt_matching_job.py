"""
Job per abbinare i giocatori di `giocatore` (webAPP_FantaManageriale) ai profili
Transfermarkt e mantenere aggiornati data di nascita e scadenza contratto reale.

Pensato per girare periodicamente (workflow .github/workflows/transfermarkt_matching.yml):
1. uno step precedente del workflow scarica la rosa Serie A con transfermarkt-scraper
   (github.com/dcaribou/transfermarkt-scraper) e produce un file JSON (una riga per
   giocatore, crawler `players`);
2. questo script legge quel file e aggiorna il DB.

Uso:
    python transfermarkt_matching_job.py --input players.json

Per i giocatori senza `id_transfermarkt` fa SOLO il match esatto su club + cognome
(+ iniziale quando serve): auto-commit se un solo candidato, marca le righe come "in
revisione" (tabella transfermarkt_giocatori) se 2+ o 0. Non tenta match fuzzy: quello
resta nella dashboard admin dell'app (webAPP_FantaManageriale), che calcola i
suggerimenti al volo dalla cache invece di scriverli qui.

Per i giocatori che hanno GIÀ un `id_transfermarkt` (assegnato in un run precedente),
aggiorna solo `data_nascita`/`scadenza_contratto` se cambiati: `id_transfermarkt`, una
volta trovato, non viene mai più toccato (l'associazione non va ricalcolata se un
giocatore cambia squadra: la ricerca dell'aggiornamento avviene per id_transfermarkt,
stabile nel tempo, non per club).

Protetto da controlli di sicurezza: si rifiuta di scrivere qualunque cosa (nessuna
modifica al DB) se il dump scaricato ha troppi pochi giocatori o troppi pochi club
rispetto all'atteso, per non rovinare i dati in caso di scraping fallito/incompleto.
"""

import argparse
import json
import os
import re
import sys

import psycopg2
from psycopg2.extras import RealDictCursor

from transfermarkt_matching_utils import candidati_esatti, parse_data_tm

# Stesse credenziali usate dagli altri job di questo repo (vedi backup_sb.py,
# fc_to_sb_to_gs_ETL.py): host/user/porta fissi, solo la password viene dal secret.
SUPABASE_HOST = "aws-1-eu-central-1.pooler.supabase.com"
SUPABASE_PORT = 6543
SUPABASE_DB = "postgres"
SUPABASE_USER = "postgres.vhowswomnwhbfdpslsep"

RE_ID_GIOCATORE = re.compile(r"/spieler/(\d+)")

# Oggi un dump completo di Serie A ha ~593 giocatori su 20 club: soglie larghe per
# non bloccare su piccole variazioni di rosa, ma abbastanza strette da bloccare
# su uno scraping palesemente fallito/incompleto.
SOGLIA_MINIMA_GIOCATORI = 400
MASSIMO_CLUB_MANCANTI = 2


class DumpNonAffidabile(Exception):
    """Il dump scaricato sembra incompleto/corrotto: nessuna scrittura viene eseguita."""


def connetti():
    password = os.environ.get("SUPABASE_PASSWORD")
    if not password:
        raise RuntimeError("Variabile d'ambiente SUPABASE_PASSWORD non impostata.")
    return psycopg2.connect(
        host=SUPABASE_HOST,
        database=SUPABASE_DB,
        user=SUPABASE_USER,
        password=password,
        port=SUPABASE_PORT,
    )


def carica_giocatori_transfermarkt(percorso_input):
    """Legge il dump JSON di transfermarkt-scraper e raggruppa per nome club TM."""
    per_club_tm = {}

    with open(percorso_input, encoding="utf-8") as f:
        for riga in f:
            riga = riga.strip()
            if not riga:
                continue
            dato = json.loads(riga)

            match_id = RE_ID_GIOCATORE.search(dato.get("href") or "")
            if not match_id:
                continue

            club_tm = (dato.get("parent") or {}).get("name")
            cognome = dato.get("last_name") or dato.get("name") or ""

            per_club_tm.setdefault(club_tm, []).append({
                "id_transfermarkt": int(match_id.group(1)),
                "cognome": cognome,
                "nome": dato.get("name") or "",
                "nome_completo": f"{dato.get('name') or ''} {cognome}".strip(),
                "club_tm": club_tm,
                "data_nascita": parse_data_tm(dato.get("date_of_birth")),
                "scadenza_contratto": parse_data_tm(dato.get("contract_expires")),
            })

    return per_club_tm


def salva_cache(cur, giocatori_tm_per_club_tm):
    """Svuota transfermarkt_giocatori e la ripopola col dump più recente (tutti i
    giocatori partono come sola cache, id_giocatore NULL: i marcatori di revisione
    vengono rimessi dal resto di esegui_matching nella stessa transazione)."""
    cur.execute("TRUNCATE transfermarkt_giocatori;")
    for club_tm, giocatori in giocatori_tm_per_club_tm.items():
        for g in giocatori:
            cur.execute(
                """
                INSERT INTO transfermarkt_giocatori
                    (id_transfermarkt, club_tm, nome, cognome, data_nascita, scadenza_contratto)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (g["id_transfermarkt"], club_tm, g["nome"], g["cognome"],
                 g["data_nascita"], g["scadenza_contratto"]),
            )


def aggiorna_gia_matchati(cur, per_id_transfermarkt):
    """Aggiorna data_nascita/scadenza_contratto dei giocatori già mappati in un run
    precedente, SENZA mai toccare id_transfermarkt: una volta trovato, resta fisso.
    La ricerca è per id_transfermarkt (stabile), non per club: un trasferimento di un
    giocatore da un club di Serie A a un altro non rompe l'associazione."""
    cur.execute(
        '''
        SELECT id, id_transfermarkt, data_nascita, scadenza_contratto
        FROM giocatore
        WHERE id_transfermarkt IS NOT NULL AND priorita = 1;
        '''
    )
    gia_matchati = cur.fetchall()

    n_aggiornati = 0
    for g in gia_matchati:
        aggiornato = per_id_transfermarkt.get(g["id_transfermarkt"])
        if not aggiornato:
            # Non più nella rosa scaricata (es. ha lasciato la Serie A): lascia i
            # dati com'erano piuttosto che cancellarli.
            continue
        if (aggiornato["data_nascita"] == g["data_nascita"]
                and aggiornato["scadenza_contratto"] == g["scadenza_contratto"]):
            continue

        cur.execute(
            '''
            UPDATE giocatore
            SET data_nascita = %s,
                scadenza_contratto = %s
            WHERE id = %s;
            ''',
            (aggiornato["data_nascita"], aggiornato["scadenza_contratto"], g["id"]),
        )
        n_aggiornati += 1

    return n_aggiornati


def esegui_matching(conn, percorso_input):
    giocatori_tm_per_club_tm = carica_giocatori_transfermarkt(percorso_input)

    totale_giocatori = sum(len(v) for v in giocatori_tm_per_club_tm.values())
    if totale_giocatori < SOGLIA_MINIMA_GIOCATORI:
        raise DumpNonAffidabile(
            f"Solo {totale_giocatori} giocatori nel dump (attesi almeno {SOGLIA_MINIMA_GIOCATORI}): "
            "probabile scraping fallito o incompleto."
        )

    per_id_transfermarkt = {
        g["id_transfermarkt"]: g
        for giocatori in giocatori_tm_per_club_tm.values()
        for g in giocatori
    }

    n_auto = n_ambigui = n_non_trovati = 0
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT club, nome_transfermarkt FROM transfermarkt_mappa_club;")
    mappa_club = {r["club"]: r["nome_transfermarkt"] for r in cur.fetchall()}

    club_mancanti = [nome for nome in mappa_club.values() if nome not in giocatori_tm_per_club_tm]
    if len(club_mancanti) > MASSIMO_CLUB_MANCANTI:
        raise DumpNonAffidabile(
            f"{len(club_mancanti)} club mancanti dal dump ({', '.join(club_mancanti)}): "
            "probabile scraping parziale."
        )

    salva_cache(cur, giocatori_tm_per_club_tm)
    n_aggiornati = aggiorna_gia_matchati(cur, per_id_transfermarkt)

    # priorita = 1: solo i giocatori attualmente in Serie A (priorita = 0 sono
    # svincolati/fuori rosa di club non più in Serie A, non cercabili su Transfermarkt
    # nelle rose attuali che scraperemo).
    cur.execute("SELECT id, nome, club FROM giocatore WHERE id_transfermarkt IS NULL AND priorita = 1;")
    nostri_giocatori = cur.fetchall()

    for giocatore in nostri_giocatori:
        club_tm = mappa_club.get(giocatore["club"])
        candidati = candidati_esatti(
            giocatore["nome"], giocatori_tm_per_club_tm.get(club_tm, [])
        )

        if len(candidati) == 1:
            c = candidati[0]
            cur.execute(
                """
                UPDATE giocatore
                SET id_transfermarkt = %s,
                    data_nascita = %s,
                    scadenza_contratto = %s
                WHERE id = %s;
                """,
                (c["id_transfermarkt"], c["data_nascita"], c["scadenza_contratto"], giocatore["id"]),
            )
            n_auto += 1

        elif len(candidati) >= 2:
            # Marca le righe di cache già inserite come candidati in revisione per
            # questo giocatore (niente righe duplicate: la tabella è stata appena
            # svuotata e ripopolata da salva_cache).
            cur.execute(
                """
                UPDATE transfermarkt_giocatori
                SET id_giocatore = %s
                WHERE id_transfermarkt = ANY(%s);
                """,
                (giocatore["id"], [c["id_transfermarkt"] for c in candidati]),
            )
            n_ambigui += 1

        else:
            # Riga sintetica "non trovato": nessun dato TM reale, solo il marcatore.
            cur.execute(
                """
                INSERT INTO transfermarkt_giocatori (id_giocatore, id_transfermarkt)
                VALUES (%s, NULL);
                """,
                (giocatore["id"],),
            )
            n_non_trovati += 1

    conn.commit()
    cur.close()

    print(f"🔄 Aggiornati (già mappati, dati cambiati): {n_aggiornati}")
    print(f"✅ Match automatico (nuovi): {n_auto}")
    print(f"⚠️  Ambigui (in revisione, dashboard admin dell'app): {n_ambigui}")
    print(f"❌ Non trovati (in revisione, dashboard admin dell'app): {n_non_trovati}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Abbina i giocatori a Transfermarkt.")
    parser.add_argument("--input", required=True, help="File JSON (newline-delimited) prodotto da transfermarkt-scraper (crawler players)")
    args = parser.parse_args()

    connessione = connetti()
    try:
        esegui_matching(connessione, args.input)
    except Exception:
        connessione.rollback()
        raise
    finally:
        connessione.close()
