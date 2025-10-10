#%%
from sqlalchemy import text
from layers.gold.utils.db_model import create_schema, create_dimensions, create_facts
from layers.gold.utils.db_config import create_engine_connection

#%%
def create_database() -> None:
    """Cria o banco de dados usando SQLAlchemy"""
    engine = create_engine_connection()
    
    drop_database(engine)
    create_schema(engine, 'dimensional')
    create_dimensions(engine, 'dimensional')
    create_facts(engine, 'dimensional')
    
    engine.dispose()

def drop_database(engine) -> None:
    """Remove o schema dimensional se existir"""
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS dimensional CASCADE;"))
        conn.commit()

#%%

