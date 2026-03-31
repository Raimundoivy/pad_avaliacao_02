# Projeto PAD

Repositório do trabalho de **Programação para Análise de Dados** do Bacharelado em Ciência da Computação do IFAM.

O projeto reúne duas frentes complementares:

- uma análise territorial aplicada com base em dados públicos do IBGE e do INEP;
- um apêndice técnico de benchmark sobre formatos de armazenamento.

## Onde Começar

- **Variante recomendada para leitura e execucao:** `notebooks/PAD_projeto_sem_snapshot.ipynb`
- **Artefato de dados exigido por essa variante:** `dados/saidas_finais/master_municipios_longo.csv`
- **Volume legado com infraestrutura de snapshot:** `notebooks/PAD_projeto.ipynb`

O repositorio adota uma arquitetura de **arquivo unico** em ambos os volumes. A diferenca e de escopo: `PAD_projeto_sem_snapshot.ipynb` trabalha diretamente sobre a base longa, enquanto `PAD_projeto.ipynb` preserva a arquitetura anterior com camada sintetica municipal e apendice tecnico de benchmark.

## O Que Há de Principal no Projeto

- **Notebook principal snapshot-free:** `notebooks/PAD_projeto_sem_snapshot.ipynb`
- **Artefato canônico dessa variante:** `dados/saidas_finais/master_municipios_longo.csv` (`4.036.741 x 21`)
- **Notebook legado:** `notebooks/PAD_projeto.ipynb`
- **Contrato atual da variante recomendada:** notebook unico com analise e validacao centradas na base longa

As bases combinam fontes oficiais do IBGE e do INEP, com integracao por codigo IBGE de 7 digitos. Na variante recomendada, as agregacoes municipais existem apenas em memoria, como apoio analitico, sem se converterem em novo artefato publico.

## Como Ler o Repositório

### `notebooks/`

Reune os volumes principais do projeto.

- `PAD_projeto_sem_snapshot.ipynb`: variante recomendada, centrada na base longa e sem infraestrutura de snapshot.
- `PAD_projeto.ipynb`: volume legado, preservado como registro da arquitetura com snapshot e benchmark.

### `dados/`

Concentra os arquivos de dados e os resultados gerados pela pipeline.

- `br_ibge_populacao_municipio.csv` e `tx_rend_municipios_2024.zip`: insumos versionados mínimos presentes no repositório.
- `cache/`: cache local das consultas e extrações de apoio.
- `intermediarios/`: arquivos auxiliares usados em etapas de tratamento.
- `legado/`: artefatos de fases anteriores do projeto.
- `saidas_finais/`: arquivos finais usados pelos notebooks, com destaque para `master_municipios_longo.csv` como base canonica da variante snapshot-free.

### `textos_contexto/`

Reúne os documentos que explicam o projeto, apoiam a leitura metodológica e registram auditorias e validações.

- `PROJETO DE PESQUISA.md`: proposta metodológica do trabalho.
- `validacao_consonancia_projeto_dois.md`: parecer de aderência ao plano revisado.
- `auditoria_conformidade_pad_avaliacao_02.md`: auditoria histórica de uma fase anterior.
- `atividade.md` e `recapitulação-do-projeto.txt`: materiais de apoio e contexto.

### `benchmarks_outputs/`

Guarda as saidas geradas pelo benchmark de formatos associado ao notebook legado.

## Relação Entre os Arquivos

- `PAD_projeto_sem_snapshot.ipynb` e a leitura preferencial do projeto: carrega `master_municipios_longo.csv`, produz agregacoes analiticas temporarias e executa a validacao da base longa.
- `PAD_projeto.ipynb` permanece como volume historico da fase em que o projeto ainda mantinha uma camada sintetica municipal e o bloco de benchmark no mesmo arquivo.
- `dados/saidas_finais/` guarda os arquivos consumidos pelos notebooks, com `master_municipios_longo.csv` como interface minima da variante recomendada.
- `textos_contexto/` reune os pareceres, auditorias e materiais de contexto que explicam a evolucao metodologica e operacional do projeto.

## Observação Sobre Arquivos Grandes

Por restrições de versionamento no GitHub, nem todos os artefatos pesados do projeto ficam armazenados diretamente no repositório. Arquivos como `*.csv`, `*.parquet` e outras saídas volumosas podem existir apenas no ambiente local.
