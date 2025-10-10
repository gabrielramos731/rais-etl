#%%
import os
import pandas as pd
import time
from layers.gold.utils.process_data import process_data
from layers.gold.config.config_gold import PATH_ESTB_SILVER, PATH_ESTB_GOLD, DIM_PATH
#%%
def run_gold_layer() -> None:
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

# %%

pd.read_parquet(os.path.join(DIM_PATH, 'merged_output.parquet'))