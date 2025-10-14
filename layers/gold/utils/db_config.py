#%%
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Database configuration
DB_CONFIG = {
    'user': 'postgres',
    'password': '2302',
    'host': 'localhost',
    'port': '5432',
    'database': 'rais'
}

def create_engine_connection() -> Engine:
    """
    Create a SQLAlchemy engine for database connections.
    
    This function builds a connection engine using the DB_CONFIG settings,
    providing a reusable database connection pool for all Gold layer operations.
    
    Returns:
        Engine: SQLAlchemy engine configured with PostgreSQL connection parameters
        
    Notes:
        - Uses connection pooling for efficient resource management
        - Connection string format: postgresql://user:password@host:port/database
        - Engine should be disposed after use with engine.dispose()
    """
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)
    return engine

def create_connection():
    """
    Return a raw SQLAlchemy connection (legacy compatibility).
    
    This function maintains backward compatibility with older code that expected
    a raw connection object instead of an engine. New code should prefer using
    create_engine_connection() directly.
    
    Returns:
        Connection: SQLAlchemy raw connection object
        
    Notes:
        - Legacy function for backward compatibility
        - New code should use create_engine_connection() instead
        - Returns a connection from the engine pool
    """
    engine = create_engine_connection()
    return engine.connect()
