# Silver Layer - Camada de Transformação

## Visão Geral

A Silver Layer realiza a transformação e modelagem dimensional dos dados. Aplica regras de negócio, enriquece os dados com informações geográficas e setoriais, e cria as tabelas de dimensões.

## Objetivo

Transformar dados conformados da Bronze Layer em um modelo dimensional, criando tabelas de dimensões e enriquecendo os dados de estabelecimentos com informações necessárias para análises posteriores.

## Processamento

### Criação de Dimensões

São criadas as seguintes tabelas dimensionais:

**dim_uf** - Unidades Federativas brasileiras

**dim_mesorregiao** - Mesorregiões geográficas do IBGE

**dim_microrregiao** - Microrregiões geográficas do IBGE

**dim_municipio** - Municípios brasileiros

**dim_cnae** - Classificação Nacional de Atividades Econômicas (completa com seções, divisões e classes)

### Enriquecimento de Dados

Os dados de estabelecimentos são enriquecidos através de merge com as dimensões criadas:
- Informações geográficas completas (UF, meso, micro, município)
- Informações setoriais (CNAE completo com hierarquia)
- Validação de integridade referencial

### Transformações aplicadas

- Join com tabelas dimensionais
- Normalização de códigos e identificadores
- Validação de foreign keys
- Remoção de registros sem correspondência nas dimensões

## Estrutura

```
silver_layer/
├── config/
│   └── config_silver.py
├── scripts/
│   └── silver_layer.py
├── utils/
│   ├── process_data.py
│   └── process_dimensions.py
└── data/
    ├── conformed/
    │   └── estabelecimentos/
    └── dimensions/
        ├── dim_uf.parquet
        ├── dim_mesorregiao.parquet
        ├── dim_microrregiao.parquet
        ├── dim_municipio.parquet
        └── dim_cnae.parquet
```

## Execução

```bash
python -m layers.silver.scripts.silver_layer
```

O processamento segue a ordem:
1. Criação das dimensões
2. Processamento dos estabelecimentos
3. Validação de integridade

## Output

**Dimensões**: Arquivos Parquet em `data/dimensions/`

**Dados enriquecidos**: Arquivos `ESTB{ANO}.parquet` em `data/conformed/estabelecimentos/` com todas as colunas das dimensões

## Qualidade de Dados

Validações aplicadas:
- Integridade referencial em todos os foreign keys
- Códigos CNAE padronizados com 5 dígitos
- Códigos geográficos validados contra base IBGE
- Alertas para registros sem correspondência
