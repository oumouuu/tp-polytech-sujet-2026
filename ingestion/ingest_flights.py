"""
Ingestion de l'entité `flights`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, utilisé comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection

# colonnes du CSV sauf la clé flight_id (comme AIRPORT_COLS dans ingest_airports.py)
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
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    # flight_date en DATE et les heures en TIME pour pouvoir calculer la durée en gold
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
    """Upsert du snapshot du jour dans silver_flights (clé : flight_id)."""
    # Même logique que les aéroports : le fichier du jour contient tous les vols,
    # certains sont modifiés et d'autres disparaissent -> upsert + désactivation.
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)
    snapshot_date = date(2025, 8, 31) if init else day

    df = df.copy()
    df["is_active"] = True

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)

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

    # les vols qui ne sont plus dans le fichier du jour sont désactivés (pas supprimés,
    # il y a des réservations dessus)
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
    """dim_flight = silver_flights + durée de vol calculée."""
    con = get_connection()
    # durée = arrivée - départ en minutes
    # si l'arrivée est avant le départ c'est un vol de nuit qui passe minuit : on ajoute 24h
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
