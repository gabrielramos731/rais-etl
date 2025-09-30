#%%
import os
import time
from layers.bronze.utils.file_normalizer import normaliza_tipos
from layers.bronze.config.config_bronze import RAW_PATH_ESTB, OUT_PATH_ESTB_BRONZE

def run_bronze_layer():
    file_list = os.listdir(RAW_PATH_ESTB)
    for file_name in file_list:
        print(f"Processando: {file_name}")
        normaliza_tipos(file_name, RAW_PATH_ESTB, OUT_PATH_ESTB_BRONZE)

if __name__ == "__main__":
    start_time = time.time()
    run_bronze_layer()
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Tempo total de execução: {elapsed:.2f} segundos")