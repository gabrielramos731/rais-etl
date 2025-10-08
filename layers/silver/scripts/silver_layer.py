#%%
import os
import pandas as pd
import time
from layers.silver.utils.process_data import processa_dados
from layers.silver.utils.process_dimensions import cria_dimensoes
from layers.silver.config.config_silver import PATH_ESTB_BRONZE, OUT_PATH_ESTB_SILVER

def run_silver_layer() -> None:
    file_list = os.listdir(PATH_ESTB_BRONZE)
    cria_dimensoes()
    for file_name in file_list:
        print(f"Processando: {file_name}")
        processa_dados(file_name, PATH_ESTB_BRONZE, OUT_PATH_ESTB_SILVER)

if __name__ == "__main__":
    start_time = time.time()
    run_silver_layer()
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Tempo total de execução: {elapsed:.2f} segundos")


# %%
