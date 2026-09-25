"""
Utilitaires partagés par les scripts d'ingestion. `get_connection` est fourni tel quel ;
`fetch_csv` est à vous d'implémenter (cf. TODO)
"""
import os
import urllib.request

import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
RAW_BASE = "https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main"


def fetch_csv(subdir: str, filename: str) -> pd.DataFrame:
    """Renvoie le contenu de <subdir>/<filename> sous forme de DataFrame pandas.

    Couche bronze : le fichier est téléchargé UNE seule fois depuis le dépôt source et écrit
    tel quel (octet pour octet, sans passer par pandas) dans bronze/<subdir>/<filename>.
    Les appels suivants relisent simplement le fichier local : pas de nouveau téléchargement,
    et le pipeline reste rejouable hors ligne une fois les fichiers rapatriés.
    """
    local_path = os.path.join(BRONZE_DIR, subdir, filename)

    if not os.path.exists(local_path):
        # exist_ok=True : ne plante pas si le dossier existe déjà (rejeu d'un jour déjà ingéré).
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        url = f"{RAW_BASE}/{subdir}/{filename}"
        # urlretrieve écrit la réponse HTTP brute sur disque : c'est exactement ce qu'on veut en
        # bronze (aucune transformation, on garde la source telle quelle).
        urllib.request.urlretrieve(url, local_path)

    # On relit toujours depuis le disque, jamais depuis la réponse HTTP : la couche bronze
    # (le fichier local) est la seule source de vérité pour la suite du pipeline.
    return pd.read_csv(local_path)


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def ensure_dim_currency():
    """Crée et peuple dim_currency si elle n'existe pas encore (script SQL fourni, idempotent).

    run_month.py l'exécute déjà avant toute ingestion, mais fact_booking en a besoin : on
    s'assure ici que chaque script peut aussi tourner seul (python ingest_bookings.py).
    """
    sql_path = os.path.join(BASE_DIR, "sql", "init", "dim_currency.sql")
    con = get_connection()
    con.execute(open(sql_path).read())
    con.close()
