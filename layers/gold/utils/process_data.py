#%%
import os
import pandas as pd
import fastparquet

def process_data(file_name, raw_path, out_path, dim_path) -> None:
    file_path = os.path.join(raw_path, file_name)

    df = merge_dimensions(file_path, dim_path)
    calculate_indexes(df)

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

    df.to_parquet(os.path.join(dim_path, 'merged_output.parquet'), index=False, engine='fastparquet')

    return df

def calculate_indexes(df) -> None:
    calculate_idx_muni(df)

    # call db save here (another function file)

def calculate_idx_muni(df):
    pass


# %%
