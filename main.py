from typing import Union
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import duckdb
    
DB_CONFIG = {
    'user': 'postgres',
    'password': '2302',
    'host': 'localhost',
    'port': '5432',
    'database': 'rais'
}

# Conexão PostgreSQL (para queries pequenas)
SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Conexão DuckDB (para queries grandes)
POSTGRES_CONNECTION = f"dbname={DB_CONFIG['database']} user={DB_CONFIG['user']} password={DB_CONFIG['password']} host={DB_CONFIG['host']} port={DB_CONFIG['port']}"

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_duckdb():
    """Conexão DuckDB para queries grandes"""
    conn = duckdb.connect(':memory:')  # ou salve em arquivo: duckdb.connect('cache.duckdb')
    
    # Instala e carrega extensão PostgreSQL
    conn.execute("INSTALL postgres;")
    conn.execute("LOAD postgres;")
    
    # Anexa o banco PostgreSQL
    conn.execute(f"""
        ATTACH '{POSTGRES_CONNECTION}' AS pg (TYPE POSTGRES, READ_ONLY);
    """)
    
    return conn

# -----

app = FastAPI()

@app.get("/")
def read_root() -> dict[str, Union[str, int]]:
    return {"message": "API is running", "version": 1}

@app.get("/test-db")
def test_database_connection(db: Session = Depends(get_db)):
    """Testa a conexão com PostgreSQL"""
    try:
        result = db.execute(text("SELECT version();"))
        version = result.fetchone()[0]
        
        result = db.execute(text("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'dimensional'
        """))
        table_count = result.fetchone()[0]
        
        return {
            "status": "connected",
            "database": DB_CONFIG['database'],
            "postgres_version": version,
            "dimensional_tables": table_count
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

# Endpoint tradicional (para queries pequenas)
@app.get("/m")
def get_municipalities(limit: int = 200000, db: Session = Depends(get_db)):
    """Query pequena usando PostgreSQL direto"""
    try:
        result = db.execute(text(f"""
            SELECT *
            FROM dimensional.fact_div_muni_mv fdmm
        """))
        
        columns = result.keys()
        data = [dict(zip(columns, row)) for row in result]
        
        return {
            "count": len(data),
            "data": data
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

# Endpoint DuckDB (para queries GRANDES)
@app.get("/ml")
def get_municipalities_large(limit: int = 200000):
    """
    Query grande usando DuckDB - sem limite de tamanho
    Processa milhões de linhas eficientemente
    """
    try:
        conn = get_duckdb()
        
        # Query otimizada no DuckDB
        result = conn.execute(f"""
            SELECT *
            FROM pg.dimensional.fact_div_muni_mv fdmm
            LIMIT {limit}
        """).fetchdf()  # Retorna pandas DataFrame
        
        conn.close()
        
        # Converte DataFrame para JSON
        return {
            "count": len(result),
            "data": result.to_dict('records')
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
    