"""
Ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, utilisé comme modèle).

Pourquoi un simple insert ici, et pas un upsert ? L'exploration montre que `bookings_<date>.csv`
ne contient QUE les réservations effectuées ce jour-là (pas de cumul, aucun booking_id commun
entre deux jours), et qu'une réservation, une fois créée, n'est jamais modifiée ni supprimée.
C'est un flux d'évènements immuables : c'est la table de FAIT du modèle. On ajoute donc les
lignes du jour, sans mise à jour ni désactivation.

`ON CONFLICT DO NOTHING` sert uniquement à rendre la fonction rejouable : si le pipeline est
relancé sur un jour déjà ingéré, les booking_id déjà présents sont ignorés au lieu d'être
dupliqués (idempotence).
"""
from datetime import date

from common import ensure_dim_currency, fetch_csv, get_connection

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
    """Rapatrie le fichier bookings du jour (ou d'init/) dans bronze/, tel quel."""
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    # Pas de is_active / deleted_date : un évènement ne se désactive pas.
    # insert_timestamp et update_timestamp sont conservés par cohérence avec les autres
    # tables ; comme une réservation n'est jamais retouchée, ils resteront toujours égaux.
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
    """Relit le fichier du jour depuis bronze/ et insère ses lignes dans silver_bookings."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)  # déjà en cache local : pas de nouveau téléchargement

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)

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
    """Reconstruit fact_booking, puis la table d'agrégation agg_revenue_daily_airline.

    fact_booking : une ligne par réservation (grain = la réservation), avec les clés vers les
    dimensions (passenger_id, flight_id, airport_id, currency), les mesures (amount, amount_eur)
    et deux "dimensions dégénérées" (seat_class, booking_channel : trop simples pour justifier
    une table à part). amount_eur = amount * rate_to_eur, via dim_currency.

    La table d'agrégation vit ici car elle dépend de fact_booking ET de dim_flight (pour la
    compagnie) : dans run_month.py, le gold des réservations est exécuté en dernier, donc
    dim_flight est déjà à jour au moment où on la construit.
    """
    ensure_dim_currency()

    con = get_connection()

    con.execute("""
        CREATE OR REPLACE TABLE fact_booking AS
        SELECT
            b.booking_id,
            b.booking_date,
            b.passenger_id,
            b.flight_id,
            b.airport_id,                                   -- aéroport de départ du vol réservé
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

    # Chiffre d'affaires (en EUR) et volume, par jour de réservation et par compagnie.
    # LEFT JOIN : une réservation dont le vol aurait été supprimé côté source reste comptée,
    # grâce à la désactivation (et non suppression) des vols en silver.
    # Table entièrement recalculée à chaque exécution : ses colonnes techniques datent donc de
    # la dernière reconstruction, pas de la première apparition de chaque ligne.
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
