# Práticas com Formatos de Dados e Manipulação de Grandes Volumes

Repositório destinado ao 1º Trabalho da disciplina de **Programação para Análise de Dados** do Bacharelado em Ciência da Computação (Instituto Federal de Educação, Ciência e Tecnologia do Amazonas - IFAM).

## Objetivo do Projeto
Consolidar o conhecimento sobre serialização de arquivos, compressão de dados, encoding estrito e o impacto das arquiteturas de armazenamento (Orientação a Linha *vs* Orientação a Coluna, Processamento em Disco *vs* Memória). 

Neste projeto, é realizada uma implementação prática com medições rigorosas de R/W, R/W Column Level, Compressão, além de processamento *Big Data* utilizando diferentes codecs (como Snappy, Gzip, Zstd) em múltiplos formatos amplamente difundidos na indústria global (como Avro, Parquet, Arrow/Feather, ORC e formatos clássicos como CSV, JSON e XML).

## Conjunto de Dados (Dataset Principal)
O conjunto utilizado para os testes práticos de _benchmarking_ possui as seguintes características:
- **Origem/Fonte:** Instituto Brasileiro de Geografia e Estatística (IBGE).
- **Finalidade:** Conjunto massivo contendo indicadores socioeconômicos e de produção agrícola a nível municipal cruzados com informações de matrículas rurais em nível de ensino médio.
- **Volume Consolidado:** 4.252.903 linhas em 13 colunas (~400 MB flat-size).
- **Recorte Temporal Diacrônico:** Período referencial com safras compreendendo as datas de 1974 a 2024.
- **Construção e ETL:** A rotina utilizada que condensa os arquivos do IBGE foi idealizada pelo time através da pipeline embutida no Jupyter Notebook secundário (`notebooks/pad_avaliacao_02.ipynb`).

*Observação: Por restrições de limite de tamanho de versionamento global no GitHub (arquivos com mais de 100MB), a pasta de artefatos local e os volumes textuais de origem não foram commitados no repositório (`*.csv`, `*.parquet`, etc).*

## Estrutura do Repositório

```text
├── benchmarks.ipynb                # O Notebook principal de benchmarking executado
├── notebooks/                      # Código do processamento ETL
│   ├── pad_avaliacao_02.ipynb      # ETL da limpeza e extração da base do IBGE
│   └── pesquisa_pad_trabalho.ipynb # Jupyter Scratch de pesquisas acadêmicas / ensaios
├── textos_contexto/                # Diretório secundário de referências teóricas
└── dados/ (Não rastreado/Gitignored) # Diretório das saídas locais CSV e parquet brutos
```

## Referências
As menções bibliográficas com papers das criações dos sistemas de arquivos assim como a documentação estrutural da Apache Software Foundation estão minuciosamente citados nas células `Markdown` do index principal de `benchmarks`.
