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
    dim['divisao'] = dim['divisao'].astype('str')

    dim.to_parquet(os.path.join(config_silver.DIM_OUT_PATH, 'dim_cnae.parquet'), index=False)

def cria_dim_ano() -> None:
    '''Cria a dimensão ano'''
    dim_ano = pd.DataFrame({'ano': list(range(2007, 2030))})
    dim_ano['ano'] = dim_ano['ano'].astype(int)
    dim_ano['id_ano'] = dim_ano.index + 1
    dim_ano.to_parquet(os.path.join(config_silver.DIM_OUT_PATH, 'dim_ano.parquet'), index=False)

def altera_tipos_regiao() -> None:
    '''Altera o tipo de dados das colunas ids nas dimensões de regiao e persiste os arquivos'''
    paths = {
        'dim_municipio': os.path.join(config_silver.DIM_OUT_PATH, 'dim_municipio.parquet'),
        'dim_microrregiao': os.path.join(config_silver.DIM_OUT_PATH, 'dim_microrregiao.parquet'),
        'dim_mesorregiao': os.path.join(config_silver.DIM_OUT_PATH, 'dim_mesorregiao.parquet'),
        'dim_uf': os.path.join(config_silver.DIM_OUT_PATH, 'dim_uf.parquet'),
    }

    # Leitura dos arquivos (se existirem)
    dfs = {name: pd.read_parquet(path) for name, path in paths.items()}

    # Mapas de conversão por dataframe; usaremos dtype "string" para garantir texto persistente
    conversions = {
        'dim_municipio': {'id_municipio': 'string', 'id_microrregiao': 'string'},
        'dim_microrregiao': {'id_microrregiao': 'string', 'id_mesorregiao': 'string'},
        'dim_mesorregiao': {'id_mesorregiao': 'string', 'id_uf': 'string'},
        'dim_uf': {'id_uf': 'string'},
    }

    for name, df in dfs.items():
        conv = {col: dtype for col, dtype in conversions.get(name, {}).items() if col in df.columns}
        if conv:
            df = df.astype(conv)
        df.to_parquet(paths[name], index=False)

#%%
cria_dim_ano()
cria_dim_cnae()
altera_tipos_regiao()