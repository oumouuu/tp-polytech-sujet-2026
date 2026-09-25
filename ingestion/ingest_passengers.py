"""
Ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, utilisé comme modèle).

Il y a deux fichiers sources (EN et FR) qui n'ont pas le même format : on les met au même
format dans le silver, dans une seule table silver_passengers.
"""
from datetime import date

import pandas as pd

from common import fetch_csv, get_connection

# correspondance des colonnes FR -> EN (on garde les noms anglais comme schéma final)
RENOMMAGE_FR = {
    "id_passager": "passenger_id",
    "prenom": "first_name",
    "nom": "last_name",
    "genre": "gender",
    "nationalite": "nationality",
    "email": "email",
    "date_naissance": "birth_date",
    "date_inscription": "signup_date",
}
GENRE_FR = {"Homme": "Male", "Femme": "Female"}

# colonnes de la table silver sauf la clé
# source_system : "EN" ou "FR", pour savoir de quel fichier vient la ligne
PASSENGER_COLS = [
    "first_name",
    "last_name",
    "gender",
    "nationality",
    "email",
    "birth_date",
    "signup_date",
    "source_system",
]

# date à laquelle on calcule l'âge (fin de la période simulée), pour que le résultat
# ne change pas à chaque fois qu'on relance le pipeline
DATE_REF = date(2025, 9, 30)


def _snapshot_files(day: date = None, init: bool = False):
    if init:
        return "init", "passengers_en.csv", "passengers_fr.csv"
    assert day is not None
    d = day.isoformat()
    return "2025-09", f"passengers_en_{d}.csv", f"passengers_fr_{d}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    subdir, fichier_en, fichier_fr = _snapshot_files(day, init)
    fetch_csv(subdir, fichier_en)
    fetch_csv(subdir, fichier_fr)


def _fusionner(df_en, df_fr):
    """Met les deux fichiers au même format et les colle l'un sous l'autre."""
    en = df_en.copy()
    fr = df_fr.rename(columns=RENOMMAGE_FR)

    # Homme/Femme -> Male/Female
    fr["gender"] = fr["gender"].replace(GENRE_FR)

    # dates : le FR est en JJ/MM/AAAA, on remet tout en AAAA-MM-JJ
    # (on donne le format à pandas pour ne pas confondre le jour et le mois)
    for col in ["birth_date", "signup_date"]:
        fr[col] = pd.to_datetime(fr[col], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
        en[col] = pd.to_datetime(en[col], format="%Y-%m-%d").dt.strftime("%Y-%m-%d")

    en["source_system"] = "EN"
    fr["source_system"] = "FR"

    colonnes = ["passenger_id"] + PASSENGER_COLS
    return pd.concat([en[colonnes], fr[colonnes]], ignore_index=True)


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers (
            passenger_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            source_system VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Upsert des deux snapshots (EN + FR) dans silver_passengers (clé : passenger_id)."""
    subdir, fichier_en, fichier_fr = _snapshot_files(day, init)
    df = _fusionner(fetch_csv(subdir, fichier_en), fetch_csv(subdir, fichier_fr))
    snapshot_date = date(2025, 8, 31) if init else day

    df["is_active"] = True

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)

    # même upsert que les aéroports et les vols
    update_cols = PASSENGER_COLS + ["is_active"]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    set_clause += ", deleted_date = NULL, update_timestamp = now()"

    con.execute(f"""
        INSERT INTO silver_passengers (
            passenger_id, {", ".join(update_cols)}, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT passenger_id, {", ".join(update_cols)}, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (passenger_id) DO UPDATE SET {set_clause}
    """)

    # passagers disparus du fichier -> désactivés
    con.execute(
        """
        UPDATE silver_passengers SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND passenger_id NOT IN (SELECT passenger_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.unregister("snapshot")
    con.close()


def ingest_gold():
    """dim_passenger = silver_passengers + âge et tranche d'âge."""
    con = get_connection()
    # age() de DuckDB donne l'âge exact (jour et mois compris)
    # les tranches sont un choix libre (le prof a dit que le découpage n'avait pas d'importance)
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_passenger AS
        WITH tmp AS (
            SELECT
                *,
                CAST(date_part('year', age(?::DATE, birth_date)) AS INTEGER) AS age
            FROM silver_passengers
        )
        SELECT
            *,
            CASE
                WHEN age < 18 THEN '<18'
                WHEN age < 25 THEN '18-24'
                WHEN age < 35 THEN '25-34'
                WHEN age < 45 THEN '35-44'
                WHEN age < 55 THEN '45-54'
                WHEN age < 65 THEN '55-64'
                ELSE '65+'
            END AS age_bracket
        FROM tmp
        """,
        [DATE_REF],
    )
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
