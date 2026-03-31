# Práticas com Formatos de Dados e Manipulação de Grandes Volumes

Repositório destinado ao 1º Trabalho da disciplina de **Programação para Análise de Dados** do Bacharelado em Ciência da Computação (Instituto Federal de Educação, Ciência e Tecnologia do Amazonas - IFAM).

## Objetivo do Projeto
Consolidar o conhecimento sobre serialização de arquivos, compressão de dados, encoding estrito e o impacto das arquiteturas de armazenamento (Orientação a Linha *vs* Orientação a Coluna, Processamento em Disco *vs* Memória). 

Neste projeto, é realizada uma implementação prática com medições rigorosas de R/W, R/W Column Level, Compressão, além de processamento *Big Data* utilizando diferentes codecs (como Snappy, Gzip, Zstd) em múltiplos formatos amplamente difundidos na indústria global (como Avro, Parquet, Arrow/Feather, ORC e formatos clássicos como CSV, JSON e XML).

## Conjunto de Dados
O projeto hoje convive com duas camadas de artefatos:

- **Artefatos finais atuais:** `master_municipios_longo.csv` (`4.036.741 x 21`) e `master_municipios_analitico_snapshot.csv` (`5.570 x 39`), gerados pela pipeline analítica em `notebooks/pad_avaliacao_02.ipynb`.
- **Artefato legado de benchmarking:** `master_municipios_tratado.csv` (`4.252.902 x 13`), mantido para comparação histórica e para o notebook de benchmark.

As fontes combinam bases oficiais do IBGE e do INEP, com integração municipal por código IBGE de 7 dígitos na versão atual da pipeline. O snapshot analítico passou a expor sinais booleanos auditáveis e uma tipologia explícita de `regime_territorial`, alinhada ao PROJETO DE PESQUISA DOIS.

*Observação: Por restrições de limite de tamanho de versionamento global no GitHub (arquivos com mais de 100MB), a pasta de artefatos local e os volumes textuais de origem não foram commitados no repositório (`*.csv`, `*.parquet`, etc).*

## Estrutura do Repositório

```text
├── notebooks/
│   ├── benchmarks.ipynb
│   ├── pad_avaliacao_02.ipynb
│   ├── pad_avaliacao_02_dicionario_validacao.ipynb
│   ├── build_pad_avaliacao_02_notebook.py
│   └── build_pad_avaliacao_02_dicionario_validacao.py
├── textos_contexto/
├── dados/
│   ├── cache/                      # Cache local das consultas oficiais
│   ├── intermediarios/             # CSVs auxiliares e tratamentos antigos
│   ├── legado/                     # Bases consolidadas anteriores e tabelas antigas
│   └── saidas_finais/              # Artefatos finais vigentes da refatoração
└── benchmarks_outputs/             # Saídas de serialização e compressão do benchmark
```

## Referências
As menções bibliográficas com papers das criações dos sistemas de arquivos assim como a documentação estrutural da Apache Software Foundation estão minuciosamente citados nas células `Markdown` do notebook `benchmarks.ipynb`. A documentação metodológica fica centralizada em `pad_avaliacao_02.ipynb`, enquanto o dicionário e a validação dos artefatos finais ficam em `pad_avaliacao_02_dicionario_validacao.ipynb`.
