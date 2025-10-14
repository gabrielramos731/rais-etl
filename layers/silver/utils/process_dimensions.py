#%%
import os
import pandas as pd
from layers.silver.config import config_silver

def cria_dimensoes() -> None:
    """
    Executes all dimension creation functions.
    
    This is the main orchestrator function that calls all dimension creation
    functions in the correct order:
    1. Creates CNAE dimension (economic activity classification)
    2. Creates Year dimension
    3. Converts ID column types in geographic dimensions
    
    Returns:
        None: Saves all dimension files as Parquet in the output directory
    """
    cria_dim_cnae()
    cria_dim_ano()
    altera_tipos_regiao()

def cria_dim_cnae() -> None:
    """
    Creates the CNAE dimension from CSV dictionary file.
    
    This function:
    - Reads CNAE 2.0 classification dictionary
    - Removes duplicate classes
    - Creates numeric section codes from section descriptions
    - Zero-pads class codes to 5 digits
    - Converts division codes to string
    
    Output columns:
        - classe: 5-digit CNAE class code (e.g., '01234')
        - divisao: Division code (string)
        - descricao_divisao: Division description
        - descricao_secao: Section description
        - secao: Numeric section code (1-based index)
        
    Returns:
        None: Saves dim_cnae.parquet to the dimensions output directory
    """
    dim = pd.read_csv(os.path.join(config_silver.DIM_RAW_PATH, 'dicionario_cnae_2.csv'),
                     usecols=['classe', 'divisao', 'descricao_divisao', 'descricao_secao'])
    
    dim = dim.drop_duplicates(subset='classe')
    dim['secao'] = dim['descricao_secao'].astype('category').cat.codes + 1
    dim['classe'] = dim['classe'].astype(str).str.zfill(5)
    dim['divisao'] = dim['divisao'].astype('str')

    dim.to_parquet(os.path.join(config_silver.DIM_OUT_PATH, 'dim_cnae.parquet'), index=False)

def cria_dim_ano() -> None:
    """
    Creates the Year dimension table.
    
    Generates a dimension table containing years from 2007 to 2029 with:
    - ano: Year value (2007-2029)
    - id_ano: Sequential ID starting from 1
    
    This allows for future years to be included in the data model without
    requiring schema changes.
    
    Returns:
        None: Saves dim_ano.parquet to the dimensions output directory
    """
    dim_ano = pd.DataFrame({'ano': list(range(2007, 2030))})
    dim_ano['ano'] = dim_ano['ano'].astype(int)
    dim_ano['id_ano'] = dim_ano.index + 1
    dim_ano.to_parquet(os.path.join(config_silver.DIM_OUT_PATH, 'dim_ano.parquet'), index=False)

def altera_tipos_regiao() -> None:
    """
    Converts ID column types in geographic dimensions to string type.
    
    This function:
    - Reads all dimension Parquet files (municipality, microregion, mesoregion, state, CNAE)
    - Converts all ID columns to pandas 'string' dtype for consistency
    - Persists the updated files back to Parquet format
    
    Type conversions applied:
        dim_municipio: id_municipio, id_microrregiao → string
        dim_microrregiao: id_microrregiao, id_mesorregiao → string
        dim_mesorregiao: id_mesorregiao, id_uf → string
        dim_uf: id_uf → string
        dim_cnae: secao → string
    
    Notes:
        - Uses pandas 'string' dtype (not 'object') for proper string storage
        - Only converts columns that exist in each DataFrame
        - Overwrites original files with updated types
        
    Returns:
        None: Updates and saves all dimension files in place
    """
    paths = {
        'dim_municipio': os.path.join(config_silver.DIM_OUT_PATH, 'dim_municipio.parquet'),
        'dim_microrregiao': os.path.join(config_silver.DIM_OUT_PATH, 'dim_microrregiao.parquet'),
        'dim_mesorregiao': os.path.join(config_silver.DIM_OUT_PATH, 'dim_mesorregiao.parquet'),
        'dim_uf': os.path.join(config_silver.DIM_OUT_PATH, 'dim_uf.parquet'),
        'dim_cnae': os.path.join(config_silver.DIM_OUT_PATH, 'dim_cnae.parquet'),
    }

    # Read files (if they exist)
    dfs = {name: pd.read_parquet(path) for name, path in paths.items()}

    # Conversion maps per dataframe; using 'string' dtype for persistent text storage
    conversions = {
        'dim_municipio': {'id_municipio': 'string', 'id_microrregiao': 'string'},
        'dim_microrregiao': {'id_microrregiao': 'string', 'id_mesorregiao': 'string'},
        'dim_mesorregiao': {'id_mesorregiao': 'string', 'id_uf': 'string'},
        'dim_uf': {'id_uf': 'string'},
        'dim_cnae': {'secao': 'string'},
    }

    for name, df in dfs.items():
        conv = {col: dtype for col, dtype in conversions.get(name, {}).items() if col in df.columns}
        if conv:
            df = df.astype(conv)
        df.to_parquet(paths[name], index=False)
