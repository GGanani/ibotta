import sqlite3
from sqlite3 import Error
from sqlite3 import dbapi2 as sqlite
import csv

# Opens and returns a SQLite connection to a named SQLite db
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

# Executes arbitrary SQL queries on the connected db, prints the resulting rows, and returns the number of rows
def db_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        print(row)

    return len(rows)

# Gets the schema of a chosen table
def db_getinfo(conn, tbl_name):
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + tbl_name)
    column_name_list = [tuple[0] for tuple in cur.description]

    return column_name_list

# PLEASE DESCRIBE -
def loadcsv(conn, file_name, tbl_name):
    # PLEASE DESCRIBE -
    csv_file = open(file_name)
    csv_reader = csv.DictReader(csv_file)
    insert_sql = 'INSERT INTO ' + tbl_name + ' (' + ','.join(csv_reader.fieldnames) + ') VALUES (' + ','.join(['?'] * len(csv_reader.fieldnames))+ ')'
    print(insert_sql)
    # PLEASE DESCRIBE -
    values = []
    for datarow in csv_reader:
        row_values = []
        for field in csv_reader.fieldnames:
            row_values.append(datarow[field])
        values.append(row_values)

    conn.executemany(insert_sql, values)
    conn.commit()
