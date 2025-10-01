#%%
import os
import pandas as pd
from layers.silver.config import config_silver

def cria_dimensoes():
    cria_dim_cnae()

def cria_dim_cnae():
    dim = pd.read_csv(os.path.join(config_silver.DIM_RAW_PATH, 'dicionario_cnae_2.csv'),
                     usecols=['classe', 'divisao', 'descricao_divisao', 'descricao_secao'])
    
    dim = dim.drop_duplicates(subset='classe')
    dim['secao'] = dim['descricao_secao'].astype('category').cat.codes + 1

    dim.to_parquet(os.path.join(config_silver.DIM_OUT_PATH, 'dim_cnae.parquet'), index=False)

cria_dim_cnae()
#%%

print(pd.read_parquet(os.path.join(config_silver.DIM_OUT_PATH, 'dim_cnae.parquet')))