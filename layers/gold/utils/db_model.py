#%%
def create_schema(conn, schema_name):
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    cur.close()

def create_dimensions(conn, schema):
    cur = conn.cursor()

    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.dim_uf \
                (id_uf varchar PRIMARY KEY,\
                uf varchar)")

    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.dim_mesoreegiao \
                (id_mesorregiao varchar PRIMARY KEY,\
                mesorregiao varchar,\
                id_uf varchar REFERENCES {schema}.dim_uf(id_uf))")

    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.dim_microrregiao \
                (id_microrregiao varchar PRIMARY KEY,\
                microrregiao varchar,\
                id_mesorregiao varchar REFERENCES {schema}.dim_mesoreegiao(id_mesorregiao))")

    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.dim_municipio \
                (id_municipio varchar PRIMARY KEY,\
                municipio varchar,\
                id_microrregiao varchar REFERENCES {schema}.dim_microrregiao(id_microrregiao))")

    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.dim_cnae \
                (id_classe varchar PRIMARY KEY,\
                divisao varchar,\
                descricao_divisao varchar,\
                secao varchar,\
                descricao_secao varchar)")
    conn.commit()
    cur.close()

def create_facts(conn, schema):
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.fact_sec_muni \
                (id serial PRIMARY KEY,\
                ano int,\
                id_municipio varchar REFERENCES {schema}.dim_municipio(id_municipio),\
                secao integer,\
                indice_muni_nac float,\
                indice_muni_est float)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.fact_div_muni \
                (id serial PRIMARY KEY,\
                ano int,\
                id_municipio varchar REFERENCES {schema}.dim_municipio(id_municipio),\
                divisao integer,\
                indice_muni_nac float,\
                indice_muni_est float)")
    
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.fact_sec_micro \
                (id serial PRIMARY KEY,\
                ano int,\
                id_microrregiao varchar REFERENCES {schema}.dim_microrregiao(id_microrregiao),\
                secao integer,\
                indice_micro_nac float,\
                indice_micro_est float)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.fact_div_micro \
                (id serial PRIMARY KEY,\
                ano int,\
                id_microrregiao varchar REFERENCES {schema}.dim_microrregiao(id_microrregiao),\
                divisao integer,\
                indice_micro_nac float,\
                indice_micro_est float)")
    
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.fact_sec_meso \
                (id serial PRIMARY KEY,\
                ano int,\
                id_mesorregiao varchar REFERENCES {schema}.dim_mesoreegiao(id_mesorregiao),\
                secao integer,\
                indice_meso_nac float,\
                indice_meso_est float)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.fact_div_meso \
                (id serial PRIMARY KEY,\
                ano int,\
                id_mesorregiao varchar REFERENCES {schema}.dim_mesoreegiao(id_mesorregiao),\
                divisao integer,\
                indice_meso_nac float,\
                indice_meso_est float)")
    conn.commit()
    cur.close()