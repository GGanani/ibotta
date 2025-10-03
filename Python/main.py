import ibotta_db as db
import sys


def main():

    print("Hello Ibotta!")

    base_path = "/Users/guy/Python/ibotta/"
    dir_path = base_path + "CSV_data"
    db_path = "sqlite:///" + base_path + "Database/ibotta.db"
    conn = db.create_connection(db_path)
    db.load_csv(conn, dir_path, db.map_csv(dir_path))

    query = '''
    SELECT *
    FROM customer_offer_redemptions
    LIMIT 5;
    '''
    print(db.run_sql(conn, query))

    query = '''
    SELECT *
    FROM customer_offers
    LIMIT 5;
    '''
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
