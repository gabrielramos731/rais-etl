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

def save_to_db(df1, df2, table_names) -> None:
    """Salva fatos calculados no banco
    
    Args:
        df1: DataFrame com dados da primeira tabela (seção)
        df2: DataFrame com dados da segunda tabela (divisão)
        table_names: Lista com nomes das tabelas [tabela_secao, tabela_divisao]
    """
    engine = create_engine_connection()
    
    # Salva primeira tabela (seção)
    if df1 is not None and len(table_names) > 0:
        df1.to_sql(
            name=table_names[0],
            con=engine,
            schema='dimensional',
            if_exists='append',
            index=False
        )
        print(f"✓ Inserido: {table_names[0]} ({len(df1)} registros)")
    
    # Salva segunda tabela (divisão)
    if df2 is not None and len(table_names) > 1:
        df2.to_sql(
            name=table_names[1],
            con=engine,
            schema='dimensional',
            if_exists='append',
            index=False
        )
        print(f"✓ Inserido: {table_names[1]} ({len(df2)} registros)")
    
    engine.dispose()