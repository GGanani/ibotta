from ibotta_db import IbottaDB
import sys


def main():

    print("Hello Ibotta!")
    # Where to find stuff - only works on my machine
    # This could be a known path on deployment to an EC2 instance
    base_path = "/Users/guy/Python/ibotta/"
    dir_path = base_path + "CSV_data"
    db_url = "sqlite:///" + base_path + "Database/ibotta.db"

    db = IbottaDB()
    conn = db.get_db(db_url)
    db.load_csv(conn, dir_path, db.map_csv(dir_path))

    print("Customer offers rows:")
    print(db.run_sql(conn, "SELECT COUNT(*) as ROWS FROM customer_offers;"))
    print("Customer offer rewards rows:")
    print(db.run_sql(conn, "SELECT COUNT(*) as ROWS FROM customer_offer_rewards;"))
    print("Customer offer redemptions rows:")
    print(db.run_sql(conn, "SELECT COUNT(*) as ROWS FROM customer_offer_redemptions;"))
    print("Offer rewards rows:")
    print(db.run_sql(conn, "SELECT COUNT(*) as ROWS FROM offer_rewards;"))

    query = '''
    SELECT *
    FROM customer_offers
    ORDER BY ACTIVATED DESC
    LIMIT 20;
    '''
    print("Customer offers dump:")
    print(db.run_sql(conn, query))

    print("Activations by customer:")
    print(db.run_sql_file(conn, base_path + "Queries/activations.sql"))
    print("Customers who did not activate in 2 months:")
    print(db.run_sql_file(conn, base_path + "Queries/inactive.sql"))
    print("Conversion rate by customer:")
    print(db.run_sql_file(conn, base_path + "Queries/conversion.sql"))
    print("Customer redemption values:")
    print(db.run_sql_file(conn, base_path + "Queries/redemptions.sql"))

#run main
if __name__ == '__main__':
    main()
