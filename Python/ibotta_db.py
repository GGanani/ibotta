import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Numeric, DateTime, ForeignKey
from sqlalchemy.engine import Engine
import os
import re
from typing import List, Dict, Any, Optional

# Adjust to use other DBs with sqlalchemy
db_url = "sqlite:///Database/ibotta.db"
# Careful - looks in relative path from where Python is run
dir_path = "./CSV_data"

metadata = MetaData()

# Define schemas for all tables manually to create keys
offer_rewards = Table(
    "offer_rewards",
    metadata,
    Column("ID", Integer, primary_key=True, autoincrement=True),
    Column("OFFER_ID", Integer, nullable=False),
    Column("TYPE", String),
    Column("AMOUNT", Float),
    Column("CREATED_AT", DateTime),
    Column("UPDATED_AT", DateTime),
)

customer_offers = Table(
    "customer_offers",
    metadata,
    Column("ID", Integer, primary_key=True, autoincrement=True),
    Column("CUSTOMER_ID", Integer, nullable=False),
    Column("OFFER_ID", Integer),
    Column("ACTIVATED", DateTime),
    Column("VERIFIED", DateTime),
)

customer_offer_rewards = Table(
    "customer_offer_rewards",
    metadata,
    Column("ID", Integer, primary_key=True, autoincrement=True),
    Column("CUSTOMER_ID", Integer, nullable=False),
    Column("OFFER_REWARD_ID", Integer),
    Column("FINISHED", Numeric),
    Column("CREATED_AT", DateTime),
)

customer_offer_redemptions = Table(
    "customer_offer_redemptions",
    metadata,
    Column("ID", Integer, primary_key=True, autoincrement=True),
    Column("CUSTOMER_OFFER_ID", Integer),
    Column("VERIFIED_REDEMPTION_COUNT", Integer),
    Column("SUBMITTED_REDEMPTION_COUNT", Integer),
    Column("OFFER_AMOUNT", Float),
    Column("CREATED_AT", DateTime),
)

def get_db(db_url="sqlite:///offers.db", echo=False) -> Engine:
    """
    Wrapper to create and return a SQLAlchemy engine for the given database. 
    An Engine is returned rather than a Connection because it cleanly manages
    connections on-demand and is directly supported by pandas 'to_sql'.
    SQLite file is created if it does not exist.

    Args:
        db_url (str): SQLAlchemy format database URL (e.g., "sqlite:///ibotta.db")

    Returns:
        SQLAlchemy Engine instance for connecting to the database
    """
    engine = create_engine(db_url, echo=echo)
    return engine

def init_db(engine: Engine) -> Engine:
    """
    
    Initializes the database schema.

    Args:
        conn (Engine): SQLAlchemy DB engine
    
    Returns:
        SQLAlchemy Engine instance (for chaining convenience)

    """
    metadata.drop_all(engine)
    metadata.create_all(engine)
    return engine

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

def load_csv(conn: Engine, dir: str, mapping: Dict[str, str]) -> None:
    """
    Initializes the database schema.
    Loads each csv file in a given directory into its own table as defined by a mapping.

    Args:
        conn (Engine): SQLAlchemy DB engine
        dir (str): Path to directory containing csv files
        mapping (Dict[str,str]): Mapping of csv filenames (including extension) to destination table names
    """
    init_db(conn)
    for csv_file, table_name in mapping.items():
        # Clear existing table data preserving schema
        conn.connect().execute(text(f"DELETE FROM {table_name}"))
        # Read input data from csv
        df = pd.read_csv(f"{dir}/{csv_file}", parse_dates=True)
        # Insert data without dropping the table
        df.to_sql(table_name, conn, if_exists="append", index=False)

def run_sql(conn: Engine, query: str) -> Optional[List[Dict[str, Any]]]:
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

def run_sql_file(conn: Engine, path: str) -> Optional[List[Dict[str, Any]]]:
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

    