# Projeto PAD

Repositório do trabalho de **Programação para Análise de Dados** do Bacharelado em Ciência da Computação do IFAM.

O projeto reúne duas frentes complementares:

- uma análise territorial aplicada com base em dados públicos do IBGE e do INEP;
- um apêndice técnico de benchmark sobre formatos de armazenamento.

## Onde Começar

- **Leitura principal:** `notebooks/PAD_projeto.ipynb`
- **Notebook modular da análise:** `notebooks/pad_avaliacao_02.ipynb`
- **Notebook modular de dicionário e validação:** `notebooks/pad_avaliacao_02_dicionario_validacao.ipynb`
- **Notebook modular de benchmark:** `notebooks/benchmarks.ipynb`

O volume principal `PAD_projeto.ipynb` é gerado por `notebooks/build_pad_projeto_notebook.py` a partir dos notebooks modulares.

## O Que Há de Principal no Projeto

- **Volume principal de entrega:** `notebooks/PAD_projeto.ipynb`
- **Artefatos finais atuais:** `dados/saidas_finais/master_municipios_longo.csv` (`4.036.741 x 21`) e `dados/saidas_finais/master_municipios_analitico_snapshot.csv` (`5.570 x 39`)
- **Pipeline principal:** `notebooks/pad_avaliacao_02_pipeline.py`

As bases combinam fontes oficiais do IBGE e do INEP, com integração por código IBGE de 7 dígitos. O snapshot analítico final expõe sinais booleanos auditáveis e a classificação `regime_territorial`.

## Como Ler o Repositório

### `notebooks/`

Reúne os notebooks principais do projeto e os scripts que geram os volumes consolidados.

- `PAD_projeto.ipynb`: volume final de leitura e entrega.
- `pad_avaliacao_02.ipynb`: análise principal e geração dos artefatos finais.
- `pad_avaliacao_02_dicionario_validacao.ipynb`: perfil estrutural, dicionário e validação dos arquivos finais.
- `benchmarks.ipynb`: benchmark técnico de formatos de armazenamento.
- `build_pad_projeto_notebook.py`: gera o volume final consolidado.
- `build_pad_avaliacao_02_notebook.py` e `build_pad_avaliacao_02_dicionario_validacao.py`: regeneram os notebooks modulares.

### `dados/`

Concentra os arquivos de dados e os resultados gerados pela pipeline.

- `br_ibge_populacao_municipio.csv` e `tx_rend_municipios_2024.zip`: insumos versionados mínimos presentes no repositório.
- `cache/`: cache local das consultas e extrações de apoio.
- `intermediarios/`: arquivos auxiliares usados em etapas de tratamento.
- `legado/`: artefatos de fases anteriores do projeto.
- `saidas_finais/`: arquivos finais vigentes da refatoração.

### `textos_contexto/`

Reúne os documentos que explicam o projeto, apoiam a leitura metodológica e registram auditorias e validações.

- `PROJETO DE PESQUISA.md`: proposta metodológica do trabalho.
- `validacao_consonancia_projeto_dois.md`: parecer de aderência ao plano revisado.
- `auditoria_conformidade_pad_avaliacao_02.md`: auditoria histórica de uma fase anterior.
- `atividade.md` e `recapitulação-do-projeto.txt`: materiais de apoio e contexto.

### `benchmarks_outputs/`

Guarda as saídas geradas pelo benchmark de formatos.

## Relação Entre os Arquivos

- `pad_avaliacao_02.ipynb` produz a base longa e o snapshot analítico.
- `pad_avaliacao_02_dicionario_validacao.ipynb` descreve e valida esses artefatos.
- `benchmarks.ipynb` registra a frente técnica de comparação entre formatos.
- `PAD_projeto.ipynb` reúne esses blocos em um único volume para leitura.

## Observação Sobre Arquivos Grandes

Por restrições de versionamento no GitHub, nem todos os artefatos pesados do projeto ficam armazenados diretamente no repositório. Arquivos como `*.csv`, `*.parquet` e outras saídas volumosas podem existir apenas no ambiente local.
