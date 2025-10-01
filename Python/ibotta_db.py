import pandas as pd
from sqlalchemy import create_engine
import os
import re

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