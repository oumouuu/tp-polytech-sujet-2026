"""
Ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, utilisé comme modèle).

Particularité : deux systèmes sources (un CRM anglophone, un CRM francophone) livrent chacun un
fichier, avec des schémas différents :
- noms de colonnes différents (passenger_id / id_passager, first_name / prenom, ...),
- valeurs de genre différentes (Male/Female vs Homme/Femme),
- formats de date différents (YYYY-MM-DD vs DD/MM/YYYY).
Les identifiants ne se chevauchent pas (une seule séquence globale P00001, P00002, ...), donc la
consolidation porte sur le schéma et les formats, pas sur la déduplication.

Stratégie : la couche bronze garde les deux fichiers bruts séparés ; la couche silver les
harmonise vers UN schéma unique (celui du fichier EN, majoritaire) puis les charge dans une seule
table silver_passengers avec la même logique upsert + désactivation que les vols (les passagers
sont un référentiel qui évolue : ajouts quotidiens, corrections, rares suppressions).
"""
from datetime import date

import pandas as pd

from common import fetch_csv, get_connection

# Schéma cible = schéma du fichier EN. Le fichier FR est renommé vers ce schéma.
FR_TO_EN_COLUMNS = {
    "id_passager": "passenger_id",
    "prenom": "first_name",
    "nom": "last_name",
    "genre": "gender",
    "nationalite": "nationality",
    "email": "email",
    "date_naissance": "birth_date",
    "date_inscription": "signup_date",
}
FR_TO_EN_GENDER = {"Homme": "Male", "Femme": "Female"}

# Colonnes métier de la table silver, hors clé (passenger_id).
# source_system est ajoutée par nous : savoir de quel CRM vient chaque ligne est utile pour
# le debug et la traçabilité (ex. une anomalie qui ne toucherait que le flux FR).
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

# Date de référence pour le calcul de l'âge en gold : fin de la période simulée. Un âge calculé
# par rapport à "aujourd'hui" changerait à chaque exécution du pipeline, ce qui rendrait les
# résultats non reproductibles d'une machine (ou d'un mois) à l'autre.
REFERENCE_DATE = date(2025, 9, 30)


def _snapshot_files(day: date = None, init: bool = False):
    """Renvoie (subdir, fichier_en, fichier_fr) du jour demandé."""
    if init:
        return "init", "passengers_en.csv", "passengers_fr.csv"
    assert day is not None
    d = day.isoformat()
    return "2025-09", f"passengers_en_{d}.csv", f"passengers_fr_{d}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie les deux snapshots (EN et FR) du jour dans bronze/, tels quels."""
    subdir, file_en, file_fr = _snapshot_files(day, init)
    fetch_csv(subdir, file_en)
    fetch_csv(subdir, file_fr)


def _harmonize(df_en: pd.DataFrame, df_fr: pd.DataFrame) -> pd.DataFrame:
    """Met les deux sources au même schéma et les empile en un seul DataFrame."""
    en = df_en.copy()
    fr = df_fr.rename(columns=FR_TO_EN_COLUMNS)  # rename renvoie une copie

    # Genre : on aligne les valeurs FR sur les valeurs EN.
    fr["gender"] = fr["gender"].replace(FR_TO_EN_GENDER)

    # Dates : on convertit tout en texte ISO (YYYY-MM-DD). Le format est donné explicitement à
    # pandas pour éviter toute ambiguïté jour/mois (08/01/1964 = 8 janvier, pas 1er août).
    # DuckDB convertira ensuite ces chaînes ISO en DATE à l'insertion.
    for col in ["birth_date", "signup_date"]:
        fr[col] = pd.to_datetime(fr[col], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
        en[col] = pd.to_datetime(en[col], format="%Y-%m-%d").dt.strftime("%Y-%m-%d")

    en["source_system"] = "EN"
    fr["source_system"] = "FR"

    # Même ordre de colonnes pour les deux, puis empilement vertical.
    cols = ["passenger_id"] + PASSENGER_COLS
    return pd.concat([en[cols], fr[cols]], ignore_index=True)


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
    """Relit les deux snapshots depuis bronze/, les harmonise, et upsert dans silver_passengers."""
    subdir, file_en, file_fr = _snapshot_files(day, init)
    df = _harmonize(fetch_csv(subdir, file_en), fetch_csv(subdir, file_fr))
    snapshot_date = date(2025, 8, 31) if init else day

    df["is_active"] = True

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)

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

    # Passagers disparus des fichiers du jour : désactivés, jamais supprimés (leurs réservations
    # passées doivent rester rattachables).
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
    """Reconstruit dim_passenger à partir de silver_passengers, avec âge et tranche d'âge calculés.

    age(REFERENCE_DATE, birth_date) donne un âge exact (tient compte du jour et du mois, pas
    seulement de l'année). Le découpage en tranches est un choix métier libre ; celui-ci est
    classique en marketing et donne des groupes de taille comparable sur ce jeu de données.
    """
    con = get_connection()
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_passenger AS
        WITH with_age AS (
            SELECT
                *,
                CAST(date_part('year', age(?::DATE, birth_date)) AS INTEGER) AS age_years
            FROM silver_passengers
        )
        SELECT
            *,
            CASE
                WHEN age_years < 18 THEN '<18'
                WHEN age_years < 25 THEN '18-24'
                WHEN age_years < 35 THEN '25-34'
                WHEN age_years < 45 THEN '35-44'
                WHEN age_years < 55 THEN '45-54'
                WHEN age_years < 65 THEN '55-64'
                ELSE '65+'
            END AS age_bracket
        FROM with_age
        """,
        [REFERENCE_DATE],
    )
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
