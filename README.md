# RAIS Pipeline - Análise de Especialização Econômica Regional

## Visão Geral

Pipeline ETL para processamento e análise de dados da RAIS (Relação Anual de Informações Sociais) com cálculo de Índices de Localização (Quociente Locacional) para análise de especialização econômica regional no Brasil.

## Objetivo

Criar um Data Warehouse dimensional para análise da concentração e especialização de atividades econômicas em diferentes níveis geográficos (município, microrregião, mesorregião).

Aplicações:
- Identificar vocações econômicas regionais
- Mapear clusters industriais e de serviços
- Analisar evolução temporal da especialização (2007-2024)
- Subsidiar políticas públicas de desenvolvimento regional
- Realizar benchmarking entre regiões

## Arquitetura

O projeto segue a arquitetura Medallion (Bronze, Silver, Gold):

```
FONTES DE DADOS (MTE + Base dos Dados)
    ↓
BRONZE LAYER
    - Ingestão de dados brutos
    - Normalização de formatos
    - Conversão para Parquet
    ↓
SILVER LAYER
    - Criação de dimensões
    - Enriquecimento de dados
    - Validação de integridade
    ↓
GOLD LAYER
    - Cálculo de Quociente Locacional
    - Modelo dimensional (Star Schema)
    - Persistência em PostgreSQL
```

## Estrutura do Projeto

```
rais_2/
├── layers/
│   ├── bronze/        # Ingestão
│   ├── silver/        # Transformação
│   └── gold/          # Analítica
├── dicionarios/       # Dados auxiliares
├── etl.py            # Executor completo
└── README.md
```

## Execução

### Pré-requisitos

- Python 3.12+
- PostgreSQL 12+
- Dependências: `pandas`, `sqlalchemy`, `psycopg2-binary`, `fastparquet`, `pyarrow`

```bash
pip install pandas sqlalchemy psycopg2-binary fastparquet pyarrow
```

### Configuração

**1. Criar banco de dados**

```sql
CREATE DATABASE rais;
```

**2. Configurar credenciais** em `layers/gold/utils/db_config.py`

```python
DB_CONFIG = {
    'user': 'postgres',
    'password': 'sua_senha',
    'host': 'localhost',
    'port': '5432',
    'database': 'rais'
}
```

**3. Configurar caminhos de dados** nos arquivos `config/*.py` de cada layer

### Executar Pipeline

**Opção 1: Pipeline completo (recomendado)**

Executa todas as camadas sequencialmente com relatório de progresso:

```bash
python etl.py
```

Saída esperada:
```
============================================================
INICIANDO PIPELINE ETL - RAIS
============================================================

[1/3] Executando Bronze Layer...
------------------------------------------------------------
Processando arquivos...
Bronze Layer concluída em 1200.45 segundos

[2/3] Executando Silver Layer...
------------------------------------------------------------
Criando dimensões...
Silver Layer concluída em 850.32 segundos

[3/3] Executando Gold Layer...
------------------------------------------------------------
Calculando índices...
Gold Layer concluída em 980.15 segundos

============================================================
PIPELINE ETL CONCLUÍDO COM SUCESSO
============================================================
Bronze Layer: 1200.45s
Silver Layer: 850.32s
Gold Layer:   980.15s
Total:        3030.92s (50.5 minutos)
============================================================
```

**Opção 2: Execução por layer individual**

Útil para reprocessamento ou debugging:

```bash
# Bronze Layer apenas
python -m layers.bronze.scripts.bronze_layer

# Silver Layer apenas
python -m layers.silver.scripts.silver_layer

# Gold Layer apenas
python -m layers.gold.scripts.gold_layer
```

**Opção 3: Bronze Layer com paralelização**

Acelera o processamento usando múltiplas threads:

```bash
# Usa todas as CPUs disponíveis
python -m layers.bronze.scripts.bronze_layer

# Especifica número de threads (recomendado: 4-8)
python -m layers.bronze.scripts.bronze_layer 8
```

## Indicadores Calculados

### Quociente Locacional (QL)

Mede a especialização de uma atividade econômica em determinada região.

```
QL = (Participação do setor na região) / (Participação do setor no país/estado)
```

Níveis de agregação:
- Geográfico: Município, Microrregião, Mesorregião
- Setorial: Seção CNAE, Divisão CNAE
- Comparação: Nacional e Estadual

## Documentação Detalhada

- [Bronze Layer](layers/bronze/README.md)
- [Silver Layer](layers/silver/README.md)
- [Gold Layer](layers/gold/README.md)

## Tecnologias

- Python 3.12+
- pandas
- PostgreSQL
- SQLAlchemy
- fastparquet / pyarrow
- concurrent.futures

## Autor

Gabriel Ramos - [@gabrielramos731](https://github.com/gabrielramos731)
