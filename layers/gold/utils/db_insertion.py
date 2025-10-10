#%%
import os
import pandas as pd
from layers.gold.utils.db_config import create_engine_connection
from layers.gold.config.config_gold import DIM_PATH

def insert_dimensions() -> None:
    """Insere dimensões no banco usando SQLAlchemy"""
    engine = create_engine_connection()
    
    # ordem de inserção (respeitando foreign keys)
    dim_list = ['dim_uf', 'dim_mesorregiao', 'dim_microrregiao', 'dim_municipio', 'dim_cnae']
    
    for dim_name in dim_list:
        dim = pd.read_parquet(os.path.join(DIM_PATH, dim_name + '.parquet'))
        dim.to_sql(
            name=dim_name,
            con=engine,
            schema='dimensional',
            if_exists='append',
            index=False
        )
        print(f"✓ Inserido: {dim_name} ({len(dim)} registros)")
    
    engine.dispose()

def save_to_db(ql_sec_muni, ql_div_muni) -> None:
    """Salva fatos calculados no banco"""
    engine = create_engine_connection()
    
    # Salva fact_sec_muni
    if ql_sec_muni is not None:
        ql_sec_muni.to_sql(
            name='fact_sec_muni',
            con=engine,
            schema='dimensional',
            if_exists='append',
            index=False
        )
        print(f"✓ Inserido: fact_sec_muni ({len(ql_sec_muni)} registros)")
    
    # Salva fact_div_muni
    if ql_div_muni is not None:
        ql_div_muni.to_sql(
            name='fact_div_muni',
            con=engine,
            schema='dimensional',
            if_exists='append',
            index=False
        )
        print(f"✓ Inserido: fact_div_muni ({len(ql_div_muni)} registros)")
    
    engine.dispose()