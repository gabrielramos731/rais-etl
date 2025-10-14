#%%
from sqlalchemy import text
from layers.gold.utils.db_model import create_schema, create_dimensions, create_facts
from layers.gold.utils.db_config import create_engine_connection

#%%
def create_database() -> None:
    """
    Initialize the complete database structure for the Gold layer.
    
    This function orchestrates the full database setup process:
    1. Drops existing 'dimensional' schema (CASCADE removes all objects)
    2. Creates fresh 'dimensional' schema
    3. Creates all dimension tables with foreign key relationships
    4. Creates all fact tables for location quotient metrics
    
    Returns:
        None
        
    Notes:
        - Destructive operation: drops existing schema if present
        - Creates clean slate for each ETL run
        - Ensures schema consistency by recreating from scratch
        - Engine is properly disposed after use
        
    Warning:
        This will DELETE all existing data in the dimensional schema.
        Use with caution in production environments.
    """
    engine = create_engine_connection()
    
    drop_database(engine)
    create_schema(engine, 'dimensional')
    create_dimensions(engine, 'dimensional')
    create_facts(engine, 'dimensional')
    
    engine.dispose()

def drop_database(engine) -> None:
    """
    Remove the dimensional schema and all its objects.
    
    This function performs a cascading drop of the dimensional schema,
    removing all tables, views, and other database objects within it.
    
    Args:
        engine: SQLAlchemy engine with database connection
        
    Notes:
        - Uses CASCADE to drop all dependent objects
        - IF EXISTS clause prevents errors if schema doesn't exist
        - Changes are automatically committed
        
    Warning:
        This is a destructive operation that cannot be undone.
        All data in the dimensional schema will be permanently lost.
    """
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS dimensional CASCADE;"))
        conn.commit()

