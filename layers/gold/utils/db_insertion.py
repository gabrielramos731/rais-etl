#%%
import os
import pandas as pd
from layers.gold.utils.db_config import create_engine_connection
from layers.gold.config.config_gold import DIM_PATH

def insert_dimensions() -> None:
    """
    Insert dimension data from Silver layer into PostgreSQL database.
    
    This function loads dimension parquet files created by the Silver layer
    and inserts them into the dimensional schema in the correct order to
    respect foreign key constraints.
    
    Insertion order (respecting dependencies):
    1. dim_uf (states - no dependencies)
    2. dim_mesorregiao (depends on dim_uf)
    3. dim_microrregiao (depends on dim_mesorregiao)
    4. dim_municipio (depends on dim_microrregiao)
    5. dim_cnae (economic classification - no dependencies)
    
    Returns:
        None
        
    Notes:
        - Uses pandas.to_sql() with SQLAlchemy engine for bulk insertion
        - Prints confirmation for each dimension with record count
        - Engine is properly disposed after use
        - Uses 'append' mode assuming tables are already created
    """
    engine = create_engine_connection()
    
    # Insertion order respecting foreign keys
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
    """
    Save calculated location quotient (Quociente Locacional) facts to database.
    
    This function persists analytical results (indices) to fact tables in the
    dimensional schema. It handles two related fact tables simultaneously
    (typically section-level and division-level indices for the same geography).
    
    Args:
        df1: DataFrame with first fact table data (typically section-level indices)
        df2: DataFrame with second fact table data (typically division-level indices)
        table_names: List with two table names [section_table, division_table]
        
    Notes:
        - Uses pandas.to_sql() with SQLAlchemy for bulk insertion
        - Both DataFrames use 'append' mode (tables must exist)
        - Prints confirmation with record counts for each table
        - Engine is properly disposed after use
        - Handles None values gracefully (skips if DataFrame is None)
    """
    engine = create_engine_connection()
    
    # Save first table (section)
    if df1 is not None and len(table_names) > 0:
        df1.to_sql(
            name=table_names[0],
            con=engine,
            schema='dimensional',
            if_exists='append',
            index=False
        )
        print(f"✓ Inserido: {table_names[0]} ({len(df1)} registros)")
    
    # Save second table (division)
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