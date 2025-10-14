#%%
from sqlalchemy import text

def create_schema(engine, schema_name):
    """
    Create a PostgreSQL schema if it doesn't exist.
    
    Args:
        engine: SQLAlchemy engine with database connection
        schema_name: Name of the schema to create (e.g., 'dimensional')
        
    Notes:
        - Uses CREATE SCHEMA IF NOT EXISTS to avoid errors
        - Changes are automatically committed
        - Schema creation is idempotent
    """
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        conn.commit()

def create_dimensions(engine, schema):
    """
    Create dimension tables in the specified schema.
    
    This function creates five dimension tables following a star schema design:
    - dim_uf: Brazilian states
    - dim_mesorregiao: Mesoregions (groups of microregions)
    - dim_microrregiao: Microregions (groups of municipalities)
    - dim_municipio: Municipalities (cities)
    - dim_cnae: Economic activity classification (CNAE)
    
    Args:
        engine: SQLAlchemy engine with database connection
        schema: Schema name where tables will be created
        
    Notes:
        - Tables follow hierarchical foreign key relationships:
          UF → Mesoregion → Microregion → Municipality
        - Uses varchar for IDs to preserve leading zeros (e.g., '01' for state codes)
        - All tables use IF NOT EXISTS for idempotent creation
        - CNAE dimension includes both section and division levels
    """
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.dim_uf (
                id_uf varchar PRIMARY KEY,
                uf varchar
            )
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.dim_mesorregiao (
                id_mesorregiao varchar PRIMARY KEY,
                mesorregiao varchar,
                id_uf varchar REFERENCES {schema}.dim_uf(id_uf)
            )
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.dim_microrregiao (
                id_microrregiao varchar PRIMARY KEY,
                microrregiao varchar,
                id_mesorregiao varchar REFERENCES {schema}.dim_mesorregiao(id_mesorregiao)
            )
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.dim_municipio (
                id_municipio varchar PRIMARY KEY,
                nome varchar,
                id_microrregiao varchar REFERENCES {schema}.dim_microrregiao(id_microrregiao)
            )
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.dim_cnae (
                classe varchar PRIMARY KEY,
                divisao varchar,
                descricao_divisao varchar,
                secao varchar,
                descricao_secao varchar
            )
        """))
        
        conn.commit()

def create_facts(engine, schema):
    """
    Create fact tables for location quotient (Quociente Locacional) metrics.
    
    This function creates six fact tables storing analytical results at different
    geographic and industrial classification levels:
    - fact_sec_muni: Municipality × CNAE Section indices
    - fact_div_muni: Municipality × CNAE Division indices
    - fact_sec_micro: Microregion × CNAE Section indices
    - fact_div_micro: Microregion × CNAE Division indices
    - fact_sec_meso: Mesoregion × CNAE Section indices
    - fact_div_meso: Mesoregion × CNAE Division indices
    
    Each table contains:
    - Year dimension (ano)
    - Geographic dimension (municipality/microregion/mesoregion)
    - Industry dimension (section or division)
    - National index (comparing to national average)
    - State index (comparing to state average)
    
    Args:
        engine: SQLAlchemy engine with database connection
        schema: Schema name where tables will be created
        
    Notes:
        - All tables use serial PRIMARY KEY for unique row identification
        - Foreign keys ensure referential integrity with dimension tables
        - Indices stored as float values (rounded to 3 decimal places)
        - Tables follow naming pattern: fact_{classification}_{geography}
    """
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.fact_sec_muni (
                id serial PRIMARY KEY,
                ano int,
                id_municipio varchar REFERENCES {schema}.dim_municipio(id_municipio),
                secao integer,
                indice_muni_nac float,
                indice_muni_est float
            )
        """))
        
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.fact_div_muni (
                id serial PRIMARY KEY,
                ano int,
                id_municipio varchar REFERENCES {schema}.dim_municipio(id_municipio),
                divisao integer,
                indice_muni_nac float,
                indice_muni_est float
            )
        """))
        
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.fact_sec_micro (
                id serial PRIMARY KEY,
                ano int,
                id_microrregiao varchar REFERENCES {schema}.dim_microrregiao(id_microrregiao),
                secao integer,
                indice_micro_nac float,
                indice_micro_est float
            )
        """))
        
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.fact_div_micro (
                id serial PRIMARY KEY,
                ano int,
                id_microrregiao varchar REFERENCES {schema}.dim_microrregiao(id_microrregiao),
                divisao integer,
                indice_micro_nac float,
                indice_micro_est float
            )
        """))
        
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.fact_sec_meso (
                id serial PRIMARY KEY,
                ano int,
                id_mesorregiao varchar REFERENCES {schema}.dim_mesorregiao(id_mesorregiao),
                secao integer,
                indice_meso_nac float,
                indice_meso_est float
            )
        """))
        
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.fact_div_meso (
                id serial PRIMARY KEY,
                ano int,
                id_mesorregiao varchar REFERENCES {schema}.dim_mesorregiao(id_mesorregiao),
                divisao integer,
                indice_meso_nac float,
                indice_meso_est float
            )
        """))
        
        conn.commit()