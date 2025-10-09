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


    for table in db.map_csv(dir_path).values():
        count = db.get_row_count(conn, table)
        print(f"{table.replace('_', ' ')} rows: {count}")

    query = '''
    SELECT *
    FROM customer_offers
    ORDER BY ACTIVATED DESC
    LIMIT 10;
    '''
    print("Customer offers dump:")
    print(db.run_sql(conn, query))

    print("Activations by customer:")
    print(db.run_sql_file(conn, base_path + "Queries/activations.sql")[:5])
    print("Customers who did not activate in 2 months:")
    print(db.run_sql_file(conn, base_path + "Queries/inactive.sql")[:5])
    print("Conversion rate by customer:")
    print(db.run_sql_file(conn, base_path + "Queries/conversion.sql")[:5])
    print("Customer redemption values:")
    print(db.run_sql_file(conn, base_path + "Queries/redemptions.sql")[:5])

#run main
if __name__ == '__main__':
    main()
