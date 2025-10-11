#%%
import os
import pandas as pd
import fastparquet
from layers.gold.utils.db_insertion import save_to_db
from concurrent.futures import ProcessPoolExecutor, as_completed

def process_data(file_name, raw_path, out_path, dim_path) -> None:
    file_path = os.path.join(raw_path, file_name)

    df = merge_dimensions(file_path, dim_path)
    calculate_indexes(df)

def merge_dimensions(file_path, dim_path) -> pd.DataFrame:
    df = pd.read_parquet(file_path)
    original_size = len(df)

    dim_muni = pd.read_parquet(os.path.join(dim_path, 'dim_municipio.parquet'))
    dim_micro = pd.read_parquet(os.path.join(dim_path, 'dim_microrregiao.parquet'))
    dim_meso = pd.read_parquet(os.path.join(dim_path, 'dim_mesorregiao.parquet'))
    dim_cnae = pd.read_parquet(os.path.join(dim_path, 'dim_cnae.parquet'))
    dim_uf = pd.read_parquet(os.path.join(dim_path, 'dim_uf.parquet'))

    df = pd.merge(df, dim_muni, how='inner', on='id_municipio')
    df = pd.merge(df, dim_micro, how='inner', on='id_microrregiao')
    df = pd.merge(df, dim_meso, how='inner', on='id_mesorregiao')
    df = pd.merge(df, dim_cnae, how='inner', on='classe')
    df = pd.merge(df, dim_uf, how='inner', on='id_uf')

    final_size = len(df)
    if final_size < original_size:
        lost = original_size - final_size
        print(f"⚠️ ATENÇÃO: {lost} registros removidos por falta de correspondência nas dimensões ({lost/original_size*100:.2f}%)")

    return df

def calculate_indexes(df) -> None:
    """Calcula índices em paralelo usando processos"""
    
    functions = [
        ('Município', calculate_idx_muni),
        ('Microrregião', calculate_idx_micro),
        ('Mesorregião', calculate_idx_meso)
    ]
    
    # ProcessPoolExecutor cria processos separados
    with ProcessPoolExecutor(max_workers=None) as executor:
        futures = {
            executor.submit(func, df): name 
            for name, func in functions
        }
        
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"✗ Erro em {name}: {exc}")

def calculate_idx_muni(df):
    id_cols = ['id_uf', 'id_municipio']
    secao = ['secao']
    divisao = ['divisao']

    numerador_sec_nac = df.groupby(['ano'] + id_cols + secao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_sec_nac = df.groupby(['ano'] + secao).size() / df.groupby(['ano']).size()
    
    numerador_sec_est = df.groupby(['ano'] + id_cols + secao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_sec_est = df.groupby(['ano'] + secao + ['id_uf']).size() / df.groupby(['ano', 'id_uf']).size()

    ql_sec_muni_nac = (numerador_sec_nac / denominador_sec_nac).reset_index(name='indice_muni_nac')
    ql_sec_muni_est = (numerador_sec_est / denominador_sec_est).reset_index(name='indice_muni_est')
    ql_sec_muni_nac['indice_muni_nac'] = round(ql_sec_muni_nac['indice_muni_nac'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)
    ql_sec_muni_est['indice_muni_est'] = round(ql_sec_muni_est['indice_muni_est'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)

    ql_sec_muni = pd.merge(ql_sec_muni_nac, ql_sec_muni_est, how='outer', on=['ano'] + id_cols + secao).drop(axis=1, columns=['id_uf'])

    #------------------

    numerador_div_nac = df.groupby(['ano'] + id_cols + divisao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_div_nac = df.groupby(['ano'] + divisao).size() / df.groupby(['ano']).size()
    
    numerador_div_est = df.groupby(['ano'] + id_cols + divisao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_div_est = df.groupby(['ano'] + divisao + ['id_uf']).size() / df.groupby(['ano', 'id_uf']).size()

    ql_div_muni_nac = (numerador_div_nac / denominador_div_nac).reset_index(name='indice_muni_nac')
    ql_div_muni_est = (numerador_div_est / denominador_div_est).reset_index(name='indice_muni_est')
    ql_div_muni_nac['indice_muni_nac'] = round(ql_div_muni_nac['indice_muni_nac'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)
    ql_div_muni_est['indice_muni_est'] = round(ql_div_muni_est['indice_muni_est'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)

    ql_div_muni = pd.merge(ql_div_muni_nac, ql_div_muni_est, how='outer', on=['ano'] + id_cols + divisao).drop(axis=1, columns=['id_uf'])

    #------------------

    # print(ql_sec_muni.info())

    # call db save here (another function file)
    tables = ['fact_sec_muni', 'fact_div_muni']
    save_to_db(ql_sec_muni, ql_div_muni, tables)

def calculate_idx_micro(df):
    id_cols = ['id_uf', 'id_microrregiao']
    secao = ['secao']
    divisao = ['divisao']

    # Cálculo para seção - Nacional
    numerador_sec_nac = df.groupby(['ano'] + id_cols + secao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_sec_nac = df.groupby(['ano'] + secao).size() / df.groupby(['ano']).size()
    
    # Cálculo para seção - Estadual
    numerador_sec_est = df.groupby(['ano'] + id_cols + secao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_sec_est = df.groupby(['ano'] + secao + ['id_uf']).size() / df.groupby(['ano', 'id_uf']).size()

    ql_sec_micro_nac = (numerador_sec_nac / denominador_sec_nac).reset_index(name='indice_micro_nac')
    ql_sec_micro_est = (numerador_sec_est / denominador_sec_est).reset_index(name='indice_micro_est')
    ql_sec_micro_nac['indice_micro_nac'] = round(ql_sec_micro_nac['indice_micro_nac'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)
    ql_sec_micro_est['indice_micro_est'] = round(ql_sec_micro_est['indice_micro_est'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)

    ql_sec_micro = pd.merge(ql_sec_micro_nac, ql_sec_micro_est, how='outer', on=['ano'] + id_cols + secao).drop(axis=1, columns=['id_uf'])

    # Cálculo para divisão - Nacional
    numerador_div_nac = df.groupby(['ano'] + id_cols + divisao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_div_nac = df.groupby(['ano'] + divisao).size() / df.groupby(['ano']).size()
    
    # Cálculo para divisão - Estadual
    numerador_div_est = df.groupby(['ano'] + id_cols + divisao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_div_est = df.groupby(['ano'] + divisao + ['id_uf']).size() / df.groupby(['ano', 'id_uf']).size()

    ql_div_micro_nac = (numerador_div_nac / denominador_div_nac).reset_index(name='indice_micro_nac')
    ql_div_micro_est = (numerador_div_est / denominador_div_est).reset_index(name='indice_micro_est')
    ql_div_micro_nac['indice_micro_nac'] = round(ql_div_micro_nac['indice_micro_nac'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)
    ql_div_micro_est['indice_micro_est'] = round(ql_div_micro_est['indice_micro_est'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)

    ql_div_micro = pd.merge(ql_div_micro_nac, ql_div_micro_est, how='outer', on=['ano'] + id_cols + divisao).drop(axis=1, columns=['id_uf'])

    # Salva no banco
    tables = ['fact_sec_micro', 'fact_div_micro']
    save_to_db(ql_sec_micro, ql_div_micro, tables)

def calculate_idx_meso(df):
    id_cols = ['id_uf', 'id_mesorregiao']
    secao = ['secao']
    divisao = ['divisao']

    # Cálculo para seção - Nacional
    numerador_sec_nac = df.groupby(['ano'] + id_cols + secao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_sec_nac = df.groupby(['ano'] + secao).size() / df.groupby(['ano']).size()
    
    # Cálculo para seção - Estadual
    numerador_sec_est = df.groupby(['ano'] + id_cols + secao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_sec_est = df.groupby(['ano'] + secao + ['id_uf']).size() / df.groupby(['ano', 'id_uf']).size()

    ql_sec_meso_nac = (numerador_sec_nac / denominador_sec_nac).reset_index(name='indice_meso_nac')
    ql_sec_meso_est = (numerador_sec_est / denominador_sec_est).reset_index(name='indice_meso_est')
    ql_sec_meso_nac['indice_meso_nac'] = round(ql_sec_meso_nac['indice_meso_nac'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)
    ql_sec_meso_est['indice_meso_est'] = round(ql_sec_meso_est['indice_meso_est'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)

    ql_sec_meso = pd.merge(ql_sec_meso_nac, ql_sec_meso_est, how='outer', on=['ano'] + id_cols + secao).drop(axis=1, columns=['id_uf'])

    # Cálculo para divisão - Nacional
    numerador_div_nac = df.groupby(['ano'] + id_cols + divisao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_div_nac = df.groupby(['ano'] + divisao).size() / df.groupby(['ano']).size()
    
    # Cálculo para divisão - Estadual
    numerador_div_est = df.groupby(['ano'] + id_cols + divisao).size() / df.groupby(['ano'] + id_cols).size()
    denominador_div_est = df.groupby(['ano'] + divisao + ['id_uf']).size() / df.groupby(['ano', 'id_uf']).size()

    ql_div_meso_nac = (numerador_div_nac / denominador_div_nac).reset_index(name='indice_meso_nac')
    ql_div_meso_est = (numerador_div_est / denominador_div_est).reset_index(name='indice_meso_est')
    ql_div_meso_nac['indice_meso_nac'] = round(ql_div_meso_nac['indice_meso_nac'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)
    ql_div_meso_est['indice_meso_est'] = round(ql_div_meso_est['indice_meso_est'].replace([float('inf'), -float('inf')], 0).fillna(0), 3)

    ql_div_meso = pd.merge(ql_div_meso_nac, ql_div_meso_est, how='outer', on=['ano'] + id_cols + divisao).drop(axis=1, columns=['id_uf'])

    # Salva no banco
    tables = ['fact_sec_meso', 'fact_div_meso']
    save_to_db(ql_sec_meso, ql_div_meso, tables)

# %%
