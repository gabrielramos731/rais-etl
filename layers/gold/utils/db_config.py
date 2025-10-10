#%%
import psycopg2

def create_connection() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(database="rais",
                        user="postgres",
                        password="2302",
                        host="localhost",
                        port="5432")
    return conn
