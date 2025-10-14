#%%
import os
import sys
import pandas as pd
import fastparquet

def processa_dados(file_name, raw_path, out_path) -> pd.DataFrame:
    """
    Processes Parquet files and applies appropriate transformation based on file structure.
    
    This function acts as a dispatcher that:
    - Identifies the file format by counting columns in the Parquet file
    - Applies the correct transformation (TXT or CSV format)
    - Saves the processed data to the output path
    
    Args:
        file_name (str): Name of the Parquet file to process
        raw_path (str): Directory path containing the input Parquet file
        out_path (str): Directory path where the processed file will be saved
        
    Returns:
        pd.DataFrame: Processed DataFrame (also saved to output path)
        
    File format detection:
        - 2 columns: TXT format (applies transforma_txt)
        - 3 columns: CSV format (applies transforma_csv)
    """
    file_path = os.path.join(raw_path, file_name)
    parquet_file = fastparquet.ParquetFile(file_path)
    num_cols = len(parquet_file.columns)

    if num_cols == 2: # para txt
        df = transforma_txt(file_path)
    elif num_cols == 3: # para csv
        df = transforma_csv(file_path)
    
    df.to_parquet(os.path.join(out_path, file_name), index=False, engine='fastparquet')

def transforma_txt(path) -> pd.DataFrame:
    """
    Processes TXT-format Parquet files and normalizes fields.
    
    Transformations applied:
    - Renames 'CNAE 2.0 Classe' to 'classe'
    - Renames 'Município' to 'id_municipio'
    - Extracts year from filename and adds 'ano' column
    - Converts id_municipio to string type
    - Zero-pads CNAE class to 5 digits
    
    Args:
        path (str): Full path to the Parquet file
        
    Returns:
        pd.DataFrame: Normalized DataFrame with columns [ano, classe, id_municipio]
        
    Example:
        Input: ESTB2019.parquet with columns ['Município', 'CNAE 2.0 Classe']
        Output: DataFrame with ['ano', 'classe', 'id_municipio']
    """
    df = pd.read_parquet(path)
    df.rename(columns={
        'CNAE 2.0 Classe': 'classe',
        'Município': 'id_municipio'
    }, inplace=True)

    ano = int(str.split(path, '.')[0][-4:])
    df['ano'] = ano
    df['id_municipio'] = df['id_municipio'].astype('str')
    df['classe'] = df['classe'].astype(str).str.zfill(5)
    
    return df

def transforma_csv(path) -> pd.DataFrame:
    """
    Processes CSV-format Parquet files and normalizes fields.
    
    Transformations applied:
    - Renames 'cnae_2' to 'classe'
    - Zero-pads CNAE class to 5 digits
    - Truncates municipality ID to first 6 characters
    - Special handling for ESTB2021: removes '.0' suffix from CNAE codes
    
    Args:
        path (str): Full path to the Parquet file
        
    Returns:
        pd.DataFrame: Normalized DataFrame with columns [classe, id_municipio, ...]
        
    Notes:
        - ESTB2021 file has a data quality issue where CNAE codes contain '.0'
        - This is cleaned by removing the '.0' suffix before zero-padding
        
    Example:
        Input: ESTB2020.parquet with 'cnae_2' = '1234.0'
        Output: 'classe' = '01234'
    """
    df = pd.read_parquet(path)
    df.rename(columns={
        'cnae_2': 'classe',
    }, inplace=True)

    df['classe'] = df['classe'].astype(str).str.zfill(5)
    df['id_municipio'] = df['id_municipio'].astype('str').str[:6]

    # se for arquivo ESTB2021, intercepta a leitura para remover '.0' no campo cnae_2
    if 'ESTB2021' in os.path.basename(path).upper():
        df['classe'] = df['classe'].str.replace('.0', '', regex=False)
        df['classe'] = df['classe'].str.zfill(5)

    return df
# %%
