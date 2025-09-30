#%%
import os
import sys
import pandas as pd
import fastparquet

#%%
def processa_dados(file_name, raw_path, out_path) -> pd.DataFrame:
    '''Recebe lista de arquivos parquet e identifica formato para processamento adequado'''

    file_path = os.path.join(raw_path, file_name)
    parquet_file = fastparquet.ParquetFile(file_path)
    num_cols = len(parquet_file.columns)

    if num_cols == 2: # para txt
        df = transforma_txt(file_path)
    elif num_cols == 3: # para csv
        df = transforma_csv(file_path)
    df.to_parquet(os.path.join(out_path, file_name), index=False, engine='fastparquet')

def transforma_txt(path) -> pd.DataFrame:
    '''Processa os dados parquet e normaliza os campos [ano, cnae, id_municipio]'''

    df = pd.read_parquet(path)
    df.rename(columns={
        'CNAE 2.0 Classe': 'cnae',
        'Município': 'id_municipio'
    }, inplace=True)

    ano = int(str.split(path, '.')[0][-4:])
    df['ano'] = ano
    df['id_municipio'] = df['id_municipio'].astype(str)
    df['cnae'] = df['cnae'].astype(str).str.zfill(5)
    
    return df

def transforma_csv(path) -> pd.DataFrame:
    '''Processa os dados parquet e normaliza os campos [cnae, id_municipio]'''

    df = pd.read_parquet(path)
    df.rename(columns={
        'cnae_2': 'cnae',
    }, inplace=True)

    df['cnae'] = df['cnae'].astype(str).str.zfill(5)
    df['id_municipio'] = df['id_municipio'].astype(str).str[:6]

    return df