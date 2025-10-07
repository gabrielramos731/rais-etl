#%%
import os
import pandas as pd
from layers.silver.config import config_silver

def cria_dimensoes() -> None:
    '''Executa as chamadas para criação das dimensões'''
    cria_dim_cnae()
    cria_dim_ano()

def cria_dim_cnae() -> None:
    '''Cria a dimensão cnae a partir do arquivo csv'''
    dim = pd.read_csv(os.path.join(config_silver.DIM_RAW_PATH, 'dicionario_cnae_2.csv'),
                     usecols=['classe', 'divisao', 'descricao_divisao', 'descricao_secao'])
    
    dim = dim.drop_duplicates(subset='classe')
    dim['secao'] = dim['descricao_secao'].astype('category').cat.codes + 1
    dim['classe'] = dim['classe'].astype(str).str.zfill(5)
    dim['divisao'] = dim['divisao'].astype('object')

    dim.to_parquet(os.path.join(config_silver.DIM_OUT_PATH, 'dim_cnae.parquet'), index=False)

def cria_dim_ano() -> None:
    '''Cria a dimensão ano'''
    dim_ano = pd.DataFrame({'ano': list(range(2007, 2030))})
    dim_ano['ano'] = dim_ano['ano'].astype(int)
    dim_ano['id_ano'] = dim_ano.index + 1
    dim_ano.to_parquet(os.path.join(config_silver.DIM_OUT_PATH, 'dim_ano.parquet'), index=False)

#%%
cria_dim_ano()
cria_dim_cnae()
pd.read_parquet('/home/gabriel/dev/ndti/rais_2/layers/silver/data/dimensions/dim_cnae.parquet')