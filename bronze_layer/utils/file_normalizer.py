import pandas as pd
import os

def normaliza_csv(path):
    df = pd.read_csv(path)
    return df

def normaliza_txt(path):
    df = pd.read_csv(path, encoding='latin1', sep=';', usecols=['Município', 'CNAE 2.0 Classe'])
    df = df[df['CNAE 2.0 Classe'] != '000-1']
    return df

def normaliza_tipos(file_name, raw_path, out_path):
    ext = file_name.split('.')[-1]
    file_path = os.path.join(raw_path, file_name)
    if ext == 'csv':
        df = normaliza_csv(file_path)
    elif ext == 'txt':
        df = normaliza_txt(file_path)
    else:
        raise ValueError(f"Extensão não suportada: {ext}")
    
    out_file = os.path.join(out_path, file_name.replace(ext, 'parquet'))
    print(out_file)
    df.to_parquet(out_file, engine='fastparquet')
