from sqlalchemy import text
from layers.gold.utils.db_config import create_engine_connection

def drop_materialized_view(engine, schema: str, view_name: str):
    """Remove uma view materializada se ela existir."""
    with engine.connect() as conn:
        conn.execute(text(f"""
            DROP MATERIALIZED VIEW IF EXISTS {schema}.{view_name} CASCADE
        """))
        conn.commit()


def create_fact_sec_muni_mv(engine, schema: str = "dimensional"):
    """
    View materializada: Seção CNAE × Município
    
    Combina índices de seção CNAE por município com todas as dimensões geográficas
    e informações da seção CNAE.
    """
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW {schema}.fact_sec_muni_mv AS (
                SELECT 
                    f.ano,
                    m.nome as municipio,
                    m.id_municipio,
                    micro.microrregiao,
                    micro.id_microrregiao,
                    meso.mesorregiao,
                    meso.id_mesorregiao,
                    u.uf,
                    u.id_uf,
                    dc.secao,
                    dc.descricao_secao,
                    f.indice_muni_nac,
                    f.indice_muni_est
                FROM {schema}.fact_sec_muni f
                JOIN {schema}.dim_municipio m
                    ON f.id_municipio = m.id_municipio
                JOIN {schema}.dim_microrregiao micro
                    ON m.id_microrregiao = micro.id_microrregiao
                JOIN {schema}.dim_mesorregiao meso
                    ON micro.id_mesorregiao = meso.id_mesorregiao
                JOIN {schema}.dim_uf u
                    ON meso.id_uf = u.id_uf
                INNER JOIN (
                    SELECT DISTINCT secao, descricao_secao
                    FROM {schema}.dim_cnae
                ) dc
                    ON f.secao = dc.secao::integer
            )
        """))
        conn.commit()


def create_fact_div_muni_mv(engine, schema: str = "dimensional"):
    """
    View materializada: Divisão CNAE × Município
    
    Combina índices de divisão CNAE por município com todas as dimensões geográficas
    e informações da divisão CNAE.
    """
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW {schema}.fact_div_muni_mv AS (
                SELECT 
                    f.ano,
                    m.nome as municipio,
                    m.id_municipio,
                    micro.microrregiao,
                    micro.id_microrregiao,
                    meso.mesorregiao,
                    meso.id_mesorregiao,
                    u.uf,
                    u.id_uf,
                    dc.divisao,
                    dc.descricao_divisao,
                    dc.secao,
                    dc.descricao_secao,
                    f.indice_muni_nac,
                    f.indice_muni_est
                FROM {schema}.fact_div_muni f
                JOIN {schema}.dim_municipio m
                    ON f.id_municipio = m.id_municipio
                JOIN {schema}.dim_microrregiao micro
                    ON m.id_microrregiao = micro.id_microrregiao
                JOIN {schema}.dim_mesorregiao meso
                    ON micro.id_mesorregiao = meso.id_mesorregiao
                JOIN {schema}.dim_uf u
                    ON meso.id_uf = u.id_uf
                INNER JOIN (
                    SELECT DISTINCT divisao, descricao_divisao, secao, descricao_secao
                    FROM {schema}.dim_cnae
                ) dc
                    ON f.divisao::varchar = dc.divisao
            )
        """))
        conn.commit()


def create_fact_sec_micro_mv(engine, schema: str = "dimensional"):
    """
    View materializada: Seção CNAE × Microrregião
    
    Combina índices de seção CNAE por microrregião com dimensões geográficas
    e informações da seção CNAE.
    """
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW {schema}.fact_sec_micro_mv AS (
                SELECT 
                    f.ano,
                    micro.microrregiao,
                    micro.id_microrregiao,
                    meso.mesorregiao,
                    meso.id_mesorregiao,
                    u.uf,
                    u.id_uf,
                    dc.secao,
                    dc.descricao_secao,
                    f.indice_micro_nac,
                    f.indice_micro_est
                FROM {schema}.fact_sec_micro f
                JOIN {schema}.dim_microrregiao micro
                    ON f.id_microrregiao = micro.id_microrregiao
                JOIN {schema}.dim_mesorregiao meso
                    ON micro.id_mesorregiao = meso.id_mesorregiao
                JOIN {schema}.dim_uf u
                    ON meso.id_uf = u.id_uf
                INNER JOIN (
                    SELECT DISTINCT secao, descricao_secao
                    FROM {schema}.dim_cnae
                ) dc
                    ON f.secao = dc.secao::integer
            )
        """))
        conn.commit()


def create_fact_div_micro_mv(engine, schema: str = "dimensional"):
    """
    View materializada: Divisão CNAE × Microrregião
    
    Combina índices de divisão CNAE por microrregião com dimensões geográficas
    e informações da divisão CNAE.
    """
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW {schema}.fact_div_micro_mv AS (
                SELECT 
                    f.ano,
                    micro.microrregiao,
                    micro.id_microrregiao,
                    meso.mesorregiao,
                    meso.id_mesorregiao,
                    u.uf,
                    u.id_uf,
                    dc.divisao,
                    dc.descricao_divisao,
                    dc.secao,
                    dc.descricao_secao,
                    f.indice_micro_nac,
                    f.indice_micro_est
                FROM {schema}.fact_div_micro f
                JOIN {schema}.dim_microrregiao micro
                    ON f.id_microrregiao = micro.id_microrregiao
                JOIN {schema}.dim_mesorregiao meso
                    ON micro.id_mesorregiao = meso.id_mesorregiao
                JOIN {schema}.dim_uf u
                    ON meso.id_uf = u.id_uf
                INNER JOIN (
                    SELECT DISTINCT divisao, descricao_divisao, secao, descricao_secao
                    FROM {schema}.dim_cnae
                ) dc
                    ON f.divisao::varchar = dc.divisao
            )
        """))
        conn.commit()


def create_fact_sec_meso_mv(engine, schema: str = "dimensional"):
    """
    View materializada: Seção CNAE × Mesorregião
    
    Combina índices de seção CNAE por mesorregião com dimensões geográficas
    e informações da seção CNAE.
    """
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW {schema}.fact_sec_meso_mv AS (
                SELECT 
                    f.ano,
                    meso.mesorregiao,
                    meso.id_mesorregiao,
                    u.uf,
                    u.id_uf,
                    dc.secao,
                    dc.descricao_secao,
                    f.indice_meso_nac,
                    f.indice_meso_est
                FROM {schema}.fact_sec_meso f
                JOIN {schema}.dim_mesorregiao meso
                    ON f.id_mesorregiao = meso.id_mesorregiao
                JOIN {schema}.dim_uf u
                    ON meso.id_uf = u.id_uf
                INNER JOIN (
                    SELECT DISTINCT secao, descricao_secao
                    FROM {schema}.dim_cnae
                ) dc
                    ON f.secao = dc.secao::integer
            )
        """))
        conn.commit()


def create_fact_div_meso_mv(engine, schema: str = "dimensional"):
    """
    View materializada: Divisão CNAE × Mesorregião
    
    Combina índices de divisão CNAE por mesorregião com dimensões geográficas
    e informações da divisão CNAE.
    """
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW {schema}.fact_div_meso_mv AS (
                SELECT 
                    f.ano,
                    meso.mesorregiao,
                    meso.id_mesorregiao,
                    u.uf,
                    u.id_uf,
                    dc.divisao,
                    dc.descricao_divisao,
                    dc.secao,
                    dc.descricao_secao,
                    f.indice_meso_nac,
                    f.indice_meso_est
                FROM {schema}.fact_div_meso f
                JOIN {schema}.dim_mesorregiao meso
                    ON f.id_mesorregiao = meso.id_mesorregiao
                JOIN {schema}.dim_uf u
                    ON meso.id_uf = u.id_uf
                INNER JOIN (
                    SELECT DISTINCT divisao, descricao_divisao, secao, descricao_secao
                    FROM {schema}.dim_cnae
                ) dc
                    ON f.divisao::varchar = dc.divisao
            )
        """))
        conn.commit()


def create_indexes(engine, schema: str = "dimensional"):
    """
    Cria índices para otimizar queries nas views materializadas.
    """
    indexes = [
        # fact_sec_muni_mv
        (f"{schema}.fact_sec_muni_mv", "ano"),
        (f"{schema}.fact_sec_muni_mv", "id_uf"),
        (f"{schema}.fact_sec_muni_mv", "uf"),
        (f"{schema}.fact_sec_muni_mv", "id_municipio"),
        (f"{schema}.fact_sec_muni_mv", "municipio"),
        (f"{schema}.fact_sec_muni_mv", "id_microrregiao"),
        (f"{schema}.fact_sec_muni_mv", "microrregiao"),
        (f"{schema}.fact_sec_muni_mv", "id_mesorregiao"),
        (f"{schema}.fact_sec_muni_mv", "mesorregiao"),
        (f"{schema}.fact_sec_muni_mv", "secao"),
        
        # fact_div_muni_mv
        (f"{schema}.fact_div_muni_mv", "ano"),
        (f"{schema}.fact_div_muni_mv", "id_uf"),
        (f"{schema}.fact_div_muni_mv", "uf"),
        (f"{schema}.fact_div_muni_mv", "id_municipio"),
        (f"{schema}.fact_div_muni_mv", "municipio"),
        (f"{schema}.fact_div_muni_mv", "id_microrregiao"),
        (f"{schema}.fact_div_muni_mv", "microrregiao"),
        (f"{schema}.fact_div_muni_mv", "id_mesorregiao"),
        (f"{schema}.fact_div_muni_mv", "mesorregiao"),
        (f"{schema}.fact_div_muni_mv", "divisao"),
        (f"{schema}.fact_div_muni_mv", "secao"),
        
        # fact_sec_micro_mv
        (f"{schema}.fact_sec_micro_mv", "ano"),
        (f"{schema}.fact_sec_micro_mv", "id_uf"),
        (f"{schema}.fact_sec_micro_mv", "uf"),
        (f"{schema}.fact_sec_micro_mv", "id_microrregiao"),
        (f"{schema}.fact_sec_micro_mv", "microrregiao"),
        (f"{schema}.fact_sec_micro_mv", "id_mesorregiao"),
        (f"{schema}.fact_sec_micro_mv", "mesorregiao"),
        (f"{schema}.fact_sec_micro_mv", "secao"),
        
        # fact_div_micro_mv
        (f"{schema}.fact_div_micro_mv", "ano"),
        (f"{schema}.fact_div_micro_mv", "id_uf"),
        (f"{schema}.fact_div_micro_mv", "uf"),
        (f"{schema}.fact_div_micro_mv", "id_microrregiao"),
        (f"{schema}.fact_div_micro_mv", "microrregiao"),
        (f"{schema}.fact_div_micro_mv", "id_mesorregiao"),
        (f"{schema}.fact_div_micro_mv", "mesorregiao"),
        (f"{schema}.fact_div_micro_mv", "divisao"),
        (f"{schema}.fact_div_micro_mv", "secao"),
        
        # fact_sec_meso_mv
        (f"{schema}.fact_sec_meso_mv", "ano"),
        (f"{schema}.fact_sec_meso_mv", "id_uf"),
        (f"{schema}.fact_sec_meso_mv", "uf"),
        (f"{schema}.fact_sec_meso_mv", "id_mesorregiao"),
        (f"{schema}.fact_sec_meso_mv", "mesorregiao"),
        (f"{schema}.fact_sec_meso_mv", "secao"),
        
        # fact_div_meso_mv
        (f"{schema}.fact_div_meso_mv", "ano"),
        (f"{schema}.fact_div_meso_mv", "id_uf"),
        (f"{schema}.fact_div_meso_mv", "uf"),
        (f"{schema}.fact_div_meso_mv", "id_mesorregiao"),
        (f"{schema}.fact_div_meso_mv", "mesorregiao"),
        (f"{schema}.fact_div_meso_mv", "divisao"),
        (f"{schema}.fact_div_meso_mv", "secao"),
    ]
    
    with engine.connect() as conn:
        for table, column in indexes:
            idx_name = f"idx_{table.split('.')[-1]}_{column}"
            try:
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS {idx_name}
                    ON {table} ({column})
                """))
            except Exception as e:
                print(f"Error creating {idx_name}: {str(e)}")
        conn.commit()


def create_all_materialized_views(schema: str = "dimensional"):
    """
    Cria todas as views materializadas e seus índices.
    
    Esta função é chamada automaticamente ao final do pipeline Gold layer.
    """
    
    engine = create_engine_connection()
    
    views = [
        "fact_sec_muni_mv",
        "fact_div_muni_mv", 
        "fact_sec_micro_mv",
        "fact_div_micro_mv",
        "fact_sec_meso_mv",
        "fact_div_meso_mv"
    ]
    
    for view in views:
        drop_materialized_view(engine, schema, view)
    print("\nViews existentes removidas.\n")
    
    create_fact_sec_muni_mv(engine, schema)
    create_fact_div_muni_mv(engine, schema)
    create_fact_sec_micro_mv(engine, schema)
    create_fact_div_micro_mv(engine, schema)
    create_fact_sec_meso_mv(engine, schema)
    create_fact_div_meso_mv(engine, schema)
    print("\nViews materializadas.")
    
    create_indexes(engine, schema)
    print("\nÍndices criados")

if __name__ == "__main__":
    # Executa a criação das views quando chamado diretamente
    create_all_materialized_views()
