import pytest
import pandas as pd
from ibotta_db import IbottaDB
from sqlalchemy import Engine

@pytest.fixture(scope="session")
def db():
    db = IbottaDB()
    yield db

@pytest.fixture(scope="function")
def conn(db: IbottaDB):
    conn = db.get_db(db.db_url)
    yield conn

def test_mapping(db: IbottaDB):
    # Map given CSV file names to table names
    expected = {
        "customer_offer_redemptions_31025.csv": "customer_offer_redemptions",
        "customer_offer_rewards_144392.csv": "customer_offer_rewards",
        "customer_offers_296332.csv": "customer_offers",
        "offer_rewards_168083.csv": "offer_rewards"
    }
    assert db.map_csv(db.dir_path) == expected

def test_import(db: IbottaDB, conn: Engine):
    mapping = db.map_csv(db.dir_path)

    db.load_csv(conn, db.dir_path, mapping)
    # Read a sample to check that we did it
    for table_name in mapping.values():
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", conn)
        assert not df.empty

def test_query(db: IbottaDB, conn: Engine):
    query = '''
    SELECT
        CUSTOMER_ID,
        COUNT(*) AS ACTIVATION_COUNT
    FROM customer_offers
    GROUP BY CUSTOMER_ID
    ORDER BY ACTIVATION_COUNT DESC
    LIMIT 5;
    '''
    expected = [{'CUSTOMER_ID': 40391551, 'ACTIVATION_COUNT': 1106},
                    {'CUSTOMER_ID': 40402915, 'ACTIVATION_COUNT': 1025},
                    {'CUSTOMER_ID': 40426885, 'ACTIVATION_COUNT': 982},
                    {'CUSTOMER_ID': 40429588, 'ACTIVATION_COUNT': 977},
                    {'CUSTOMER_ID': 40408527, 'ACTIVATION_COUNT': 938}]
    rows = db.run_sql(conn, query)
    assert rows == expected
    
def test_query_file(db: IbottaDB, conn: Engine):
    path = "./Queries/activations.sql"
    expected = [{'CUSTOMER_ID': 40391551, 'ACTIVATION_COUNT': 1106},
                {'CUSTOMER_ID': 40402915, 'ACTIVATION_COUNT': 1025},
                {'CUSTOMER_ID': 40426885, 'ACTIVATION_COUNT': 982},
                {'CUSTOMER_ID': 40429588, 'ACTIVATION_COUNT': 977},
                {'CUSTOMER_ID': 40408527, 'ACTIVATION_COUNT': 938}]
    rows = db.run_sql_file(conn, path)
    
    print(rows)
    assert rows == expected