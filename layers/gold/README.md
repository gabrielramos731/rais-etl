# Gold Layer - Camada Analítica

## Visão Geral

A Gold Layer realiza o cálculo de métricas analíticas e persiste os resultados em banco de dados dimensional. Esta camada produz os índices de localização (Quociente Locacional) e disponibiliza os dados para ferramentas de BI.

## Objetivo

Calcular indicadores econômicos de especialização regional e armazenar em um Data Warehouse dimensional (PostgreSQL), permitindo análises sobre concentração de atividades econômicas em diferentes níveis geográficos.

## Quociente Locacional

O Quociente Locacional (QL) mede a especialização ou concentração de uma atividade econômica em determinada região.

### Fórmula

```
QL = (E_ij / E_i) / (E_nj / E_n)

Onde:
E_ij = Emprego no setor j na região i
E_i  = Emprego total na região i
E_nj = Emprego no setor j no país/estado
E_n  = Emprego total no país/estado
```

### Interpretação

- QL > 1: Região especializada no setor
- QL = 1: Concentração média
- QL < 1: Baixa especialização

São calculadas duas variações: QL Nacional (comparação com Brasil) e QL Estadual (comparação com estado).

## Processamento

### Modelo Dimensional

O schema `dimensional` contém:

**Dimensões**: dim_uf, dim_mesorregiao, dim_microrregiao, dim_municipio, dim_cnae

**Fatos**: 
- fact_sec_muni, fact_div_muni (município)
- fact_sec_micro, fact_div_micro (microrregião)
- fact_sec_meso, fact_div_meso (mesorregião)

### Cálculo de Índices

Para cada nível geográfico:

1. Merge com todas as dimensões
2. Agregação por setor CNAE (seção e divisão)
3. Cálculo do QL nacional e estadual
4. Persistência em PostgreSQL


## Estrutura

```
gold_layer/
├── config/
│   └── config_gold.py
├── scripts/
│   └── gold_layer.py
├── utils/
│   ├── process_data.py
│   ├── db_config.py
│   ├── db_model.py
│   ├── db_start.py
│   └── db_insertion.py
└── README.md
```

## Execução

### Pré-requisitos

1. PostgreSQL instalado e rodando
2. Banco de dados criado
3. Silver Layer executada
4. Credenciais configuradas em `db_config.py`

### Configuração

```python
# db_config.py
DB_CONFIG = {
    'user': 'postgres',
    'password': 'sua_senha',
    'host': 'localhost',
    'port': '5432',
    'database': 'rais'
}
```

### Executar

```bash
python -m layers.gold.scripts.gold_layer
```

Ordem de processamento:
1. Drop e criação do schema
2. Criação de tabelas (dimensões e fatos)
3. Carga de dimensões
4. Processamento por ano (merge, cálculo, inserção)

## Output

**Schema**: `dimensional` no PostgreSQL

**Dimensões**: Dados carregados da Silver Layer

**Fatos**: Índices calculados para todos os anos, regiões e setores


## Validações

- Integridade referencial validada (foreign keys)
- Alertas para registros perdidos em merges
