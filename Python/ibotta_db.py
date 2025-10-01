import pandas as pd
from sqlalchemy import create_engine, text
import os
import re
from typing import List, Dict, Any, Optional

# Adjust to use other DBs with sqlalchemy
db_path = "sqlite:///Database/ibotta.db"
# Careful - looks in relative path from where Python is run
dir = "./CSV_data"

# Just wraps sqlalchemy: Create a SQLite database file and handle
def create_connection(db_path):
    return create_engine(db_path)

def map_csv(dir):
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

def load_csv(conn, dir, mapping):

    # Load each CSV into its own table
    for csv_file, table_name in mapping.items():
        df = pd.read_csv(dir + "/" + csv_file)
        df.to_sql(table_name, conn, if_exists="replace", index=False)

def run_sql(conn: str, query: str) -> Optional[List[Dict[str, Any]]]:
    """
    Execute arbitrary SQL against a given database.

    Args:
        conn (str): connection to a DB engine
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
        conn (str): connection to a DB engine
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
