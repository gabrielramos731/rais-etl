#%%
import os
import pandas as pd
import fastparquet

def process_data(file_name, raw_path, out_path, dim_path) -> None:
    file_path = os.path.join(raw_path, file_name)

    df = merge_dimensions(file_path, dim_path)
    calculate_indexes(df)

    # del df

def merge_dimensions(file_path, dim_path) -> pd.DataFrame:
    df = pd.read_parquet(file_path)
    dim_muni = pd.read_parquet(os.path.join(dim_path, 'dim_municipio.parquet'))
    dim_micro = pd.read_parquet(os.path.join(dim_path, 'dim_microrregiao.parquet'))
    dim_meso = pd.read_parquet(os.path.join(dim_path, 'dim_mesorregiao.parquet'))
    dim_cnae = pd.read_parquet(os.path.join(dim_path, 'dim_cnae.parquet'))
    dim_uf = pd.read_parquet(os.path.join(dim_path, 'dim_uf.parquet'))

    df = pd.merge(df, dim_muni, how='left', on='id_municipio')
    df = pd.merge(df, dim_micro, how='left', on='id_microrregiao')
    df = pd.merge(df, dim_meso, how='left', on='id_mesorregiao')
    df = pd.merge(df, dim_cnae, how='left', on='classe')
    df = pd.merge(df, dim_uf, how='left', on='id_uf')

    # df.to_parquet(os.path.join(dim_path, 'merged_output.parquet'), index=False, engine='fastparquet')

    return df

def calculate_indexes(df) -> None:
    calculate_idx_muni(df)
    # calculate_idx_micro(df)


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


    # call db save here (another function file)
    return ql_sec_muni


# %%
