#%%
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Configurações do banco
DB_CONFIG = {
    'user': 'postgres',
    'password': '2302',
    'host': 'localhost',
    'port': '5432',
    'database': 'rais'
}

def create_engine_connection() -> Engine:
    """Cria engine SQLAlchemy para uso geral"""
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)
    return engine

# Mantém compatibilidade com código antigo - agora retorna conexão do SQLAlchemy
def create_connection():
    """Retorna uma conexão SQLAlchemy (raw connection)"""
    engine = create_engine_connection()
    return engine.connect()
