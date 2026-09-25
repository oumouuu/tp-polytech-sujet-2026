"""
Ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, utilisé comme modèle).

Les réservations sont des évènements : le fichier du jour ne contient que celles du jour et
elles ne changent jamais. Donc ici c'est un simple insert, pas d'upsert ni de désactivation.
C'est la table de fait du modèle.
"""
from datetime import date

from common import fetch_csv, get_connection, init_dim_currency

BOOKING_COLS = [
    "passenger_id",
    "flight_id",
    "airport_id",
    "seat_class",
    "amount",
    "currency",
    "booking_date",
    "booking_channel",
]


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "bookings.csv"
    assert day is not None
    return "2025-09", f"bookings_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    # pas de is_active / deleted_date : une réservation ne se désactive pas
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_bookings (
            booking_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            flight_id VARCHAR,
            airport_id VARCHAR,
            seat_class VARCHAR,
            amount DOUBLE,
            currency VARCHAR,
            booking_date DATE,
            booking_channel VARCHAR,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Insert des réservations du jour dans silver_bookings."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)

    # DO NOTHING : si on relance le même jour deux fois, on ne crée pas de doublons
    cols = ", ".join(BOOKING_COLS)
    con.execute(f"""
        INSERT INTO silver_bookings (booking_id, {cols}, insert_timestamp, update_timestamp)
        SELECT booking_id, {cols}, now(), now()
        FROM snapshot
        ON CONFLICT (booking_id) DO NOTHING
    """)

    con.unregister("snapshot")
    con.close()


def ingest_gold():
    """fact_booking (avec amount_eur) + table d'agrégation par jour et compagnie."""
    init_dim_currency()

    con = get_connection()

    # une ligne par réservation, avec les clés vers les dimensions et le montant en euros
    # seat_class et booking_channel restent dans le fait (dimensions dégénérées)
    con.execute("""
        CREATE OR REPLACE TABLE fact_booking AS
        SELECT
            b.booking_id,
            b.booking_date,
            b.passenger_id,
            b.flight_id,
            b.airport_id,       -- aéroport de départ du vol
            b.currency,
            b.seat_class,
            b.booking_channel,
            b.amount,
            ROUND(b.amount * c.rate_to_eur, 2) AS amount_eur,
            b.insert_timestamp,
            b.update_timestamp
        FROM silver_bookings b
        LEFT JOIN dim_currency c ON b.currency = c.currency
    """)

    # CA par jour et par compagnie. Je la mets ici parce qu'elle a besoin de fact_booking
    # et de dim_flight, et dans run_month.py le gold des bookings est fait en dernier.
    # LEFT JOIN : les réservations sur un vol désactivé sont quand même comptées.
    con.execute("""
        CREATE OR REPLACE TABLE agg_revenue_daily_airline AS
        SELECT
            f.booking_date,
            d.airline,
            COUNT(*) AS nb_bookings,
            ROUND(SUM(f.amount_eur), 2) AS revenue_eur,
            ROUND(AVG(f.amount_eur), 2) AS avg_amount_eur,
            now() AS insert_timestamp,
            now() AS update_timestamp
        FROM fact_booking f
        LEFT JOIN dim_flight d ON f.flight_id = d.flight_id
        GROUP BY f.booking_date, d.airline
        ORDER BY f.booking_date, d.airline
    """)

    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérées.")


if __name__ == "__main__":
    init()
