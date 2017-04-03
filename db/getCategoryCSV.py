import psycopg2
import re
import json
import os
import sys

def query():
    conn = None
    try:
        # read the connection parameters
        params = "dbname='app_data' user='postgres' host='localhost' password='postgres'"
        # connect to the PostgreSQL server
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        # extract categories
        cur.execute("""SELECT DISTINCT category FROM main""")
        categories = cur.fetchall()
        print categories
        for category in categories:
            category_string = ' '.join(map(lambda s: re.sub('[(),]', '', s), category))
            print category_string
            append = """COPY (SELECT package, description FROM main WHERE category = '{}') TO '/tmp/{}.csv' With CSV DELIMITER ',';""".format(category_string, category_string)
            print "Executing "+append
            cur.execute(append)
        # close communication with the PostgreSQL database server
        print "Finish"
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    query()