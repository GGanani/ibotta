import pandas as pd
from sqlalchemy import create_engine, text
import os
import re
from typing import List, Dict, Any, Optional

# Adjust to use other DBs with sqlalchemy
db_path = "sqlite:///Database/ibotta.db"
# Careful - looks in relative path from where Python is run
dir_path = "./CSV_data"

def create_connection(db_path):
    """
    Wrapper to create and return a SQLAlchemy engine for the given database. 
    An Engine is returned rather than a Connection because it cleanly manages
    connections on-demand and is directly supported by pandas 'to_sql'.
    SQLite file is created if it does not exist.

    Args:
        db_path (str): SQLAlchemy format database URL (e.g., "sqlite:///ibotta.db")

    Returns:
        SQLAlchemy Engine instance for connecting to the database
    """
    return create_engine(db_path)

def map_csv(dir: str) -> Dict[str, str]:
    """
    Scan a directory for CSV files with names matching the pattern
    <table_name>_<digits>.csv, and return a mapping of filenames to
    table names by stripping the last underscore and digits.

    For example, a file named 'customer_offers_12345.csv' will map to:
        {'customer_offers_12345.csv': 'customer_offers'}

    Args:
        dir (str) Path to the directory containing CSV files in the known pattern

    Returns:
        Dictionary mapping each matching filename to its derived table name.

    Raises:
        FileNotFoundError: If the specified directory does not exist.
        NotADirectoryError: If the given path is not a directory.

    """
    # Regex: letters/underscores + underscore + digits + .csv
    pattern = re.compile(r"^([A-Za-z_]+)_\d+\.csv$")

    # Build mapping: filename to table name, by stripping digits
    csv_to_table = {}
    for filename in os.listdir(dir):
        match = pattern.match(filename)
        if match:
            table_name = match.group(1)
            csv_to_table[filename] = table_name

    return csv_to_table

def load_csv(conn, dir: str, mapping: Dict[str, str]) -> None:
    """
    Loads each csv file in a given directory into its own table as defined by a mapping.

    Args:
        conn (Engine): SQLAlchemy DB engine
        dir (str): Path to directory containing csv files
        mapping (Dict[str,str]): Mapping of csv filenames (including extension) to destination table names
    """
    for csv_file, table_name in mapping.items():
        df = pd.read_csv(dir + "/" + csv_file, parse_dates=True)
        df.to_sql(table_name, conn, if_exists="replace", index=False)

def run_sql(conn: str, query: str) -> Optional[List[Dict[str, Any]]]:
    """
    Execute arbitrary SQL against a given database.

    Args:
        conn (Engine): SQLAlchemy DB engine
        sql (str): SQL statement to execute

    Returns:
        list of dicts for SELECT queries, else None
    """
    with conn.connect() as db:
        result = db.execute(text(query))
        if result.returns_rows:
            return [dict(row) for row in result.mappings()]
        db.commit()

def run_sql_file(conn: str, path: str) -> Optional[List[Dict[str, Any]]]:
    """
    Execute a SQL file against a given database.

    Args:
        conn (Engine): SQLAlchemy DB engine
        path (str): path to a file to read and execute

    Returns:
        list of dicts for SELECT queries, else None
    """
    try:
        with open(path, "r") as f:
            query = f.read()
        return run_sql(conn, query)
    except FileNotFoundError as e:
        print(f"Error: {e}")

    