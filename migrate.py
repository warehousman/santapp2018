import os
import psycopg2

params = {'host': os.environ.get('HOST'), 'database': os.environ.get('DB'), 'user': os.environ.get('USER'), 'password': os.environ.get('PASS'), 'port': '5432'}

def create_tables():
    command="""
        CREATE TABLE santa (
        name      TEXT,
        uuid      UUID DEFAULT uuid_generate_v1() NOT NULL
            CONSTRAINT claus_uuid_pk
            PRIMARY KEY,
        party     TEXT,
        has_party BOOLEAN,
        real_name TEXT
        )"""
        
    conn = None
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
if __name__ == '__main__':
    create_tables()
