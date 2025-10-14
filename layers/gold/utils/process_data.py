#%%
import os
import pandas as pd
import fastparquet
from layers.gold.utils.db_insertion import save_to_db
from concurrent.futures import ProcessPoolExecutor, as_completed

def process_data(file_name, raw_path, out_path, dim_path) -> None:
    """
    Process a single establishment file through dimension merging and index calculation.
    
    This is the main orchestration function for Gold layer processing. It loads
    a Silver layer file, enriches it with all dimension data, and triggers
    parallel calculation of location quotient indices.
    
    Args:
        file_name: Name of the parquet file to process (e.g., 'ESTB2020.parquet')
        raw_path: Path to Silver layer output directory
        out_path: Path to Gold layer output directory (currently unused)
        dim_path: Path to dimension files directory
        
    Notes:
        - Reads establishment data from Silver layer
        - Merges with all five dimensions (UF, meso, micro, municipality, CNAE)
        - Spawns parallel processes for index calculation
        - Each process calculates indices for one geographic level
    """
    file_path = os.path.join(raw_path, file_name)

    df = merge_dimensions(file_path, dim_path)
    calculate_indexes(df)

def merge_dimensions(file_path, dim_path) -> pd.DataFrame:
    """
    Merge establishment data with all dimension tables.
    
    This function performs inner joins between the fact data and five dimension
    tables, creating a denormalized dataset ready for analytical calculations.
    The function uses inner joins to ensure data quality, removing records
    that don't have matching dimension values.
    
    Args:
        file_path: Full path to the establishment parquet file
        dim_path: Path to the directory containing dimension parquet files
        
    Returns:
        pd.DataFrame: Enriched dataset with all dimension attributes
        
    Notes:
        - Performs 5 inner joins: municipality → microregion → mesoregion → CNAE → UF
        - Inner joins remove records without valid dimension matches
        - Prints warning if records are lost during merge (data quality issue)
        - Loss percentage helps identify dimension data problems
    """
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
    """
    Calculate location quotient indices in parallel using separate processes.
    
    This function spawns three parallel processes to calculate location quotients
    (Quociente Locacional) at different geographic aggregation levels:
    - Municipality level (most granular)
    - Microregion level (intermediate)
    - Mesoregion level (least granular)
    
    Each process independently calculates indices for both CNAE sections and
    divisions, comparing local industrial concentration against state and
    national benchmarks.
    
    Args:
        df: Enriched DataFrame with establishment records and all dimensions
        
    Notes:
        - Uses ProcessPoolExecutor for true parallelism (CPU-bound work)
        - max_workers=None uses number of CPU cores
        - Each process is completely independent (no shared state)
        - Error handling per process prevents one failure from stopping others
        - Results are saved directly to database by each process
    """
    
    functions = [
        ('Município', calculate_idx_muni),
        ('Microrregião', calculate_idx_micro),
        ('Mesorregião', calculate_idx_meso)
    ]
    
    # ProcessPoolExecutor creates separate processes for true parallelism
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
    """
    Calculate location quotient indices at municipality level.
    
    This function computes the Location Quotient (Quociente Locacional - QL)
    for each municipality, measuring industrial concentration relative to
    state and national benchmarks. The QL indicates whether a municipality
    has higher or lower concentration of a specific industry compared to
    broader geographic areas.
    
    Args:
        df: Enriched DataFrame with establishment data and all dimensions
        
    Calculated Metrics:
        - Section level indices (broader industry classification):
          * indice_muni_nac: Municipality vs National average
          * indice_muni_est: Municipality vs State average
        - Division level indices (detailed industry classification):
          * Same metrics but at more granular CNAE division level
        
    Notes:
        - Groups by: year, UF, municipality, and industry (section/division)
        - Handles division by zero (infinity replaced with 0)
        - Handles missing values (NaN filled with 0)
        - Rounds results to 3 decimal places
        - Uses pandas division operator (/) instead of .div() for MultiIndex
        - Drops 'id_uf' column before saving (not needed in fact table)
    """
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
    """
    Calculate location quotient indices at microregion level.
    
    This function computes the Location Quotient (Quociente Locacional - QL)
    for each microregion (grouping of municipalities), measuring industrial
    concentration relative to state and national benchmarks. Microregions
    provide a mid-level geographic aggregation between municipalities and
    mesoregions.

    Args:
        df: Enriched DataFrame with establishment data and all dimensions
        
    Calculated Metrics:
        - Section level indices (broader industry classification):
          * indice_micro_nac: Microregion vs National average
          * indice_micro_est: Microregion vs State average
        - Division level indices (detailed industry classification):
          * Same metrics but at more granular CNAE division level
        
    Notes:
        - Groups by: year, UF, microregion, and industry (section/division)
        - Aggregation level: multiple municipalities per microregion
        - Follows same calculation pattern as municipality level
        - Handles infinity and NaN values (replaced with 0)
        - Rounds results to 3 decimal places
        - Geographic context: ~558 microregions in Brazil (2010 definition)
    """
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
    """
    Calculate location quotient indices at mesoregion level.
    
    This function computes the Location Quotient (Quociente Locacional - QL)
    for each mesoregion (grouping of microregions), measuring industrial
    concentration relative to state and national benchmarks. Mesoregions
    provide the broadest sub-state geographic aggregation level.
    
    Args:
        df: Enriched DataFrame with establishment data and all dimensions
        
    Calculated Metrics:
        - Section level indices (broader industry classification):
          * indice_meso_nac: Mesoregion vs National average
          * indice_meso_est: Mesoregion vs State average
        - Division level indices (detailed industry classification):
          * Same metrics but at more granular CNAE division level
        
    Notes:
        - Groups by: year, UF, mesoregion, and industry (section/division)
        - Aggregation level: multiple microregions per mesoregion
        - Broadest sub-state level (coarsest granularity)
        - Handles infinity and NaN values (replaced with 0)
        - Rounds results to 3 decimal places
        - Geographic context: ~137 mesoregions in Brazil (2010 definition)
    """
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
