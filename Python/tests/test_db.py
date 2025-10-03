import pytest
import ibotta_db as db
import pandas as pd


@pytest.fixture(scope="function")
def conn():
    conn = db.init_db(db.db_url)
    yield conn

def test_mapping():
    # Map given CSV file names to table names
    expected = {
        "customer_offer_redemptions_31025.csv": "customer_offer_redemptions",
        "customer_offer_rewards_144392.csv": "customer_offer_rewards",
        "customer_offers_296332.csv": "customer_offers",
        "offer_rewards_168083.csv": "offer_rewards"
    }
    assert db.map_csv(db.dir_path) == expected

def test_import(conn):
    mapping = db.map_csv(db.dir_path)

    db.load_csv(conn, db.dir_path, mapping)
    # Read a sample to check that we did it
    for table_name in mapping.values():
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", conn)
        assert not df.empty

def test_query(conn):
    query = '''
    SELECT
        customer_id,
        COUNT(*) AS activation_count
    FROM customer_offers
    GROUP BY customer_id
    ORDER BY activation_count DESC
    LIMIT 5;
    '''
    rows = db.run_sql(conn, query)
    assert rows == [{'CUSTOMER_ID': 40391551, 'activation_count': 1106},
                    {'CUSTOMER_ID': 40402915, 'activation_count': 1025},
                    {'CUSTOMER_ID': 40426885, 'activation_count': 982},
                    {'CUSTOMER_ID': 40429588, 'activation_count': 977},
                    {'CUSTOMER_ID': 40408527, 'activation_count': 938}]
    
def test_query_file(conn):
    path = "./Queries/activations.sql"
    rows = db.run_sql_file(conn, path)
    print(rows)
    assert rows == [{'CUSTOMER_ID': 40391551, 'activation_count': 1106},
                    {'CUSTOMER_ID': 40402915, 'activation_count': 1025},
                    {'CUSTOMER_ID': 40426885, 'activation_count': 982},
                    {'CUSTOMER_ID': 40429588, 'activation_count': 977},
                    {'CUSTOMER_ID': 40408527, 'activation_count': 938}]