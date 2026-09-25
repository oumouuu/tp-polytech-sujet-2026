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
    """Doit renvoyer le contenu de <subdir>/<filename> sous forme de DataFrame pandas."""
    chemin = os.path.join(BRONZE_DIR, subdir, filename)

    # on télécharge seulement si le fichier n'est pas déjà dans bronze/
    if not os.path.exists(chemin):
        os.makedirs(os.path.dirname(chemin), exist_ok=True)
        url = f"{RAW_BASE}/{subdir}/{filename}"
        urllib.request.urlretrieve(url, chemin)  # copie le fichier tel quel sur le disque

    # on relit toujours depuis le disque, c'est le principe du bronze
    return pd.read_csv(chemin)


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def init_dim_currency():
    # même chose que dans run_month.py, pour que ingest_bookings.py marche aussi tout seul
    sql_path = os.path.join(BASE_DIR, "sql", "init", "dim_currency.sql")
    con = get_connection()
    con.execute(open(sql_path).read())
    con.close()
