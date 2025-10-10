import pytest
from ibotta_db import IbottaDB
from sqlalchemy import Engine

@pytest.fixture(scope="session")
def db():
    db = IbottaDB()
    yield db

@pytest.fixture(scope="function")
def conn(db: IbottaDB):
    conn = db.get_db("sqlite:///:memory:")
    yield conn
    conn.dispose()


def test_inactive_query(db: IbottaDB, conn: Engine):

    # Setup and seed test data
    db.run_sql(conn, "DROP TABLE IF EXISTS customer_offers;")
    db.customer_offers.create(conn)
    seed_query = """
        INSERT INTO customer_offers (CUSTOMER_ID, OFFER_ID, ACTIVATED, VERIFIED)
        VALUES
        (1, 101,'2021-02-01 12:00:00', NULL),
        (2, 102,'2021-01-15 00:00:00', NULL),
        (3, 103, NULL, NULL)
        ;
        """
    db.run_sql(conn, seed_query)

    # Define expected output
    expected = [
        {"customer_id": 2, "last_activation": "2021-01-15 00:00:00"},
        {"customer_id": 3, "last_activation": None}
    ]

    # Compare to expected result
    path = "./Queries/inactive.sql"
    result = db.run_sql_file(conn, path)
    print(result)

    assert expected == result

def test_conversion_query(db: IbottaDB, conn: Engine):

    # Setup and seed test data
    db.run_sql(conn, "DROP TABLE IF EXISTS customer_offers;")
    db.customer_offers.create(conn)
    seed_query = """
        INSERT INTO customer_offers (CUSTOMER_ID, OFFER_ID, ACTIVATED, VERIFIED)
        VALUES
            (1, 101,'2021-02-01 12:00:00', '2021-02-15 12:00:00'),
            (1, 102,'2021-02-02 12:00:00', NULL),
            (2, 103,'2021-01-15 00:00:00', NULL),
            (2, 104,'2021-01-20 00:00:00', NULL),
            (3, 105, NULL, NULL);
        """
    db.run_sql(conn, seed_query)

    # Define expected output
    expected = [
        {"customer_id": 1, "total_offered": 2, "total_verified": 1, "conversion_rate": 0.5},
        {"customer_id": 2, "total_offered": 2, "total_verified": 0, "conversion_rate": 0.0},
        {"customer_id": 3, "total_offered": 1, "total_verified": 0, "conversion_rate": 0.0},
    ]

    # Compare to expected result
    path = "./Queries/conversion.sql"
    result = db.run_sql_file(conn, path)
    print(result)

    assert expected == result