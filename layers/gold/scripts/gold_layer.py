#%%
import os
import pandas as pd
import time
from layers.gold.utils.process_data import process_data
from layers.gold.utils.db_start import create_database
from layers.gold.utils.db_insertion import insert_dimensions
from layers.gold.config.config_gold import PATH_ESTB_SILVER, PATH_ESTB_GOLD, DIM_PATH

def run_gold_layer() -> None:
    """
    Execute the complete Gold layer ETL pipeline.
    
    This function orchestrates the Gold layer processing, which includes:
    1. Creating the database schema and fact/dimension tables
    2. Inserting dimension data from Silver layer
    3. Processing each establishment file to calculate location quotients
    4. Saving analytical results to PostgreSQL database
    
    The Gold layer produces analytical metrics (Quociente Locacional) at three
    geographic levels: municipality, microregion, and mesoregion, comparing
    industrial concentration against both state and national benchmarks.
    
    Returns:
        None
        
    Notes:
        - Requires Silver layer to be completed first
        - Creates 'dimensional' schema in PostgreSQL
        - Processes files sequentially from PATH_ESTB_SILVER
        - Each file triggers parallel index calculation (municipality, micro, meso)
    """
    create_database()
    insert_dimensions()
    file_list = os.listdir(PATH_ESTB_SILVER)
    for file_name in file_list:
        print(f"Processando: {file_name}")
        process_data(file_name, PATH_ESTB_SILVER, PATH_ESTB_GOLD, DIM_PATH)
        
if __name__ == "__main__":
    start_time = time.time()
    run_gold_layer()
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Tempo total de execução: {elapsed:.2f} segundos")
