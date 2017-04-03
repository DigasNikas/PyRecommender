import psycopg2
import json
import os
import sys

def read_jason(path):
    conn = None
    part_files = os.listdir(path)
    try:
        # read the connection parameters
        params = "dbname='app_data' user='postgres' host='localhost' password='postgres'"
        # connect to the PostgreSQL server
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        for part_file in part_files:
            part = path + part_file
            print ("reading "+part)
            with open(part, 'r') as p:
                alllines = p.readlines()
                for line in alllines:
                    json_line = ""
                    json_line = json.loads(line)
                    cur.execute('''INSERT INTO main (package,category,downloads,description,developer)
                                VALUES (%s,%s,%s,%s,%s);''',
                                (json_line['package'], json_line['category'],
                                 json_line['downloads'], json_line['description'], json_line['developer']))
                    conn.commit()
            print part + "\n"
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        print("operation commited")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("operation commited")

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """DROP TABLE main;
        """,
        """
        CREATE TABLE main (
            package VARCHAR NOT NULL,
            category VARCHAR NOT NULL,
            downloads BIGINT NOT NULL,
            description TEXT NOT NULL,
            developer VARCHAR NOT NULL
        );
        """)

    conn = None
    try:
        # read the connection parameters
        params = "dbname='app_data' user='postgres' host='localhost' password='postgres'"
        # connect to the PostgreSQL server
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()
    read_jason(sys.argv[1])