import ibotta_db as db
import sys


def main():

    print("Hello Ibotta!")

    base_path = "/Users/guy/Python/ibotta/"
    dir_path = base_path + "CSV_data"
    db_url = "sqlite:///" + base_path + "Database/ibotta.db"
    conn = db.get_db(db_url)
    db.load_csv(conn, dir_path, db.map_csv(dir_path))

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
