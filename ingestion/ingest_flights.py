"""
Ingestion de l'entité `flights`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, utilisé comme modèle).

Pourquoi un upsert + désactivation, comme pour les aéroports ? L'exploration des fichiers
journaliers montre que `flights_<date>.csv` est un extrait COMPLET du référentiel des vols, et
que d'un jour à l'autre des vols apparaissent, sont modifiés (horaire, avion, date — même
flight_id) ou disparaissent. C'est donc une dimension qui évolue, pas un flux d'évènements :
- insert si le flight_id est nouveau,
- update si le flight_id existe déjà (on garde la version la plus récente),
- désactivation (is_active = false) si le flight_id a disparu du fichier du jour, sans
  suppression physique : des réservations existent déjà sur ces vols.
"""
from datetime import date

from common import fetch_csv, get_connection

# Colonnes métier du CSV source, hors clé (flight_id). Même rôle que AIRPORT_COLS dans
# ingest_airports.py : cette liste pilote à la fois l'INSERT et le SET de l'upsert.
FLIGHT_COLS = [
    "flight_number",
    "airline",
    "origin_airport_id",
    "destination_airport_id",
    "flight_date",
    "departure_time",
    "arrival_time",
    "aircraft_type",
]


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "flights.csv"
    assert day is not None
    return "2025-09", f"flights_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le snapshot flights du jour (ou d'init/) dans bronze/, sans transformation."""
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    # flight_date en DATE et les horaires en TIME (et non en texte) : DuckDB convertit
    # automatiquement les chaînes '2025-09-22' et '10:30' du CSV à l'insertion, et on pourra
    # faire de l'arithmétique dessus en gold (durée de vol) sans reparser du texte.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights (
            flight_id VARCHAR PRIMARY KEY,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot depuis bronze/ et l'upsert dans silver_flights (clé : flight_id)."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)  # déjà en cache local : pas de nouveau téléchargement
    snapshot_date = date(2025, 8, 31) if init else day

    df = df.copy()
    df["is_active"] = True

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)

    # Upsert : nouvelles clés insérées, clés connues mises à jour avec les valeurs du jour.
    # insert_timestamp n'est jamais écrasé ; update_timestamp est rafraîchi à chaque passage ;
    # deleted_date repasse à NULL (le vol est présent aujourd'hui, donc actif).
    update_cols = FLIGHT_COLS + ["is_active"]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    set_clause += ", deleted_date = NULL, update_timestamp = now()"

    con.execute(f"""
        INSERT INTO silver_flights (
            flight_id, {", ".join(update_cols)}, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT flight_id, {", ".join(update_cols)}, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (flight_id) DO UPDATE SET {set_clause}
    """)

    # Vols absents du snapshot du jour = supprimés côté source. On les désactive seulement :
    # les réservations déjà faites sur ces vols doivent rester rattachables à leur vol.
    # Le filtre `is_active = true` rend l'opération rejouable : un second passage sur le même
    # jour ne touche plus aux lignes déjà désactivées (deleted_date reste figé).
    con.execute(
        """
        UPDATE silver_flights SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND flight_id NOT IN (SELECT flight_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.unregister("snapshot")
    con.close()


def ingest_gold():
    """Reconstruit dim_flight à partir de silver_flights, en ajoutant la durée de vol calculée.

    Piège découvert à l'exploration : 99 vols sur 350 dans init/ ont une arrival_time plus
    petite que departure_time (ex. départ 15:30, arrivée 03:41). Ce ne sont pas des erreurs
    mais des vols de nuit qui passent minuit : la source ne donne que des heures, sans date
    d'arrivée. Une soustraction naïve donnerait une durée négative ; on ajoute donc 24 h
    (1440 minutes) dans ce cas. Hypothèse assumée : aucun vol ne dure plus de 24 h.
    """
    con = get_connection()
    con.execute("""
        CREATE OR REPLACE TABLE dim_flight AS
        SELECT
            *,
            CASE
                WHEN arrival_time >= departure_time
                    THEN date_diff('minute', departure_time, arrival_time)
                ELSE date_diff('minute', departure_time, arrival_time) + 24 * 60
            END AS flight_duration_min,
            arrival_time < departure_time AS is_overnight
        FROM silver_flights
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
