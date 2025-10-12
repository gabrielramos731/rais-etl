# Bronze Layer - Camada de Ingestão

## Visão Geral

A Bronze Layer realiza a primeira etapa do pipeline ETL, responsável pela ingestão e conformação inicial dos dados brutos da RAIS. Transforma arquivos em formatos heterogêneos em um formato padronizado.

## Objetivo

Processar dados originais em formatos diferentes (TXT, CSV) e padronizá-los em Parquet, garantindo conformidade de tipos e estrutura consistente para as próximas camadas.

## Fonte de Dados

Os dados são extraídos de duas fontes:

1. **Ministério do Trabalho e Emprego** - Fonte oficial dos dados RAIS
2. **Base dos Dados** - Dados governamentais disponibilizados em formato acessível

> **Nota**: Alguns anos possuem arquivos corrompidos na fonte oficial, sendo necessário utilizar a segunda fonte. Por isso há arquivos em formatos diferentes.

## Processamento

### Operações realizadas

**Configuração de caminhos**
- Define diretórios de entrada (dados brutos)
- Define diretórios de saída (dados conformados)

**Normalização de arquivos**
- Leitura de arquivos TXT e CSV
- Padronização de nomes de colunas
- Conversão de tipos de dados
- Adição de coluna `ano` baseada no nome do arquivo

**Salvamento em Parquet**
- Conversão para formato colunar comprimido
- Armazenamento em `data/conformed/estabelecimentos/`

### Transformações aplicadas

- Renomeação: `'CNAE 2.0 Classe'` → `'cnae'`, `'Município'` → `'id_municipio'`
- Normalização CNAE: Preenchimento com zeros à esquerda (5 dígitos)
- Extração do ano do nome do arquivo
- Padronização de tipos (strings, inteiros)

## Estrutura

```
bronze_layer/
├── config/
│   └── config_bronze.py
├── scripts/
│   └── bronze_layer.py
├── utils/
│   └── file_normalizer.py
└── data/
    └── conformed/
        └── estabelecimentos/
```

## Execução

```bash
# Execução padrão
python -m layers.bronze.scripts.bronze_layer

# Com paralelização (4 threads)
python -m layers.bronze.scripts.bronze_layer 4
```

## Output

Arquivos no formato `ESTB{ANO}.parquet` em `data/conformed/estabelecimentos/`

Exemplo: `ESTB2007.parquet`, `ESTB2008.parquet`, ..., `ESTB2024.parquet`
