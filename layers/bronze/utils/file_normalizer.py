import pandas as pd
import os

def normaliza_csv(path) -> pd.DataFrame:
    """
    Reads and normalizes a CSV file.
    
    Args:
        path (str): Full path to the CSV file
        
    Returns:
        pd.DataFrame: DataFrame containing the CSV data
    """
    df = pd.read_csv(path)
    return df

def normaliza_txt(path) -> pd.DataFrame:
    """
    Reads and normalizes a TXT file with specific RAIS format.
    
    The function:
    - Reads semicolon-separated text file with latin1 encoding
    - Selects only 'Município' and 'CNAE 2.0 Classe' columns
    - Filters out records where CNAE code is '000-1' (invalid/null entries)
    
    Args:
        path (str): Full path to the TXT file
        
    Returns:
        pd.DataFrame: Filtered DataFrame with municipality and CNAE classification data
    """
    df = pd.read_csv(path, encoding='latin1', sep=';', usecols=['Município', 'CNAE 2.0 Classe'])
    df = df[df['CNAE 2.0 Classe'] != '000-1']
    return df

def normaliza_tipos(file_name, raw_path, out_path):
    """
    Processes raw data files and converts them to Parquet format.
    
    This function acts as a dispatcher that:
    - Determines file type based on extension
    - Applies appropriate normalization function (CSV or TXT)
    - Saves the result as a Parquet file in the output directory
    
    Args:
        file_name (str): Name of the file to process (e.g., 'ESTB2019.csv')
        raw_path (str): Directory path containing the raw input file
        out_path (str): Directory path where the Parquet file will be saved
        
    Raises:
        ValueError: If the file extension is not supported (must be .csv or .txt)
        
    Returns:
        None: Saves the processed file as Parquet in the output directory
        
    Example:
        normaliza_tipos('ESTB2019.csv', '/data/raw', '/data/bronze')
        # Creates /data/bronze/ESTB2019.parquet
    """
    ext = file_name.split('.')[-1]
    file_path = os.path.join(raw_path, file_name)
    if ext == 'csv':
        df = normaliza_csv(file_path)
    elif ext == 'txt':
        df = normaliza_txt(file_path)
    else:
        raise ValueError(f"Extensão não suportada: {ext}")
    
    out_file = os.path.join(out_path, file_name.replace(ext, 'parquet'))
    df.to_parquet(out_file, engine='fastparquet', index=False)
