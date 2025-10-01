import pytest
import ibotta_db as db
import pandas as pd


def test_mapping():
    dir = "./CSV_data"
    # Map given CSV file names to table names
    expected = {
        "customer_offer_redemptions_31025.csv": "customer_offer_redemptions",
        "customer_offer_rewards_144392.csv": "customer_offer_rewards",
        "customer_offers_296332.csv": "customer_offers",
        "offer_rewards_168083.csv": "offer_rewards"
    }
    assert db.map_csv(db.dir) == expected

def test_import():
    db_path = "sqlite:///Database/ibotta.db"
    dir = "./CSV_data"

    conn = db.create_connection(db.db_path)
    mapping = db.map_csv(db.dir)

    db.load_csv(conn, db.dir, mapping)
    # Read a sample to check that we did it
    for table_name in mapping.values():
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", conn)
        assert not df.empty

