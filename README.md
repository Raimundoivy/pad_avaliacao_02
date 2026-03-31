# Projeto PAD

Repositório do trabalho de **Programação para Análise de Dados** do Bacharelado em Ciência da Computação do IFAM.

O projeto reúne duas frentes complementares:

- uma análise territorial aplicada com base em dados públicos do IBGE e do INEP;
- um apêndice técnico de benchmark sobre formatos de armazenamento.

## Onde Começar

- **Volume principal de leitura e execucao:** `notebooks/PAD_projeto.ipynb`
- **Artefatos finais de dados:** `dados/saidas_finais/master_municipios_longo.csv` e `dados/saidas_finais/master_municipios_analitico_snapshot.csv`

O repositorio adota uma arquitetura de **arquivo unico**: todo o codigo operacional necessario para a leitura principal do projeto fica centralizado em `notebooks/PAD_projeto.ipynb`.

## O Que Há de Principal no Projeto

- **Volume principal de entrega:** `notebooks/PAD_projeto.ipynb`
- **Artefatos finais atuais:** `dados/saidas_finais/master_municipios_longo.csv` (`4.036.741 x 21`) e `dados/saidas_finais/master_municipios_analitico_snapshot.csv` (`5.570 x 39`)
- **Contrato atual do projeto:** notebook unico com analise, validacao e apendice tecnico no mesmo volume

As bases combinam fontes oficiais do IBGE e do INEP, com integração por código IBGE de 7 dígitos. O snapshot analítico final expõe sinais booleanos auditáveis e a classificação `regime_territorial`.

## Como Ler o Repositório

### `notebooks/`

Reune o volume principal do projeto.

- `PAD_projeto.ipynb`: volume final de leitura e entrega.
- O proprio notebook concentra a analise territorial, a validacao dos arquivos finais e o apendice tecnico de benchmark.

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

- `PAD_projeto.ipynb` e o centro do projeto: carrega os artefatos finais, produz as tabelas analiticas, executa as checagens e apresenta o benchmark no mesmo volume.
- `dados/saidas_finais/` guarda os arquivos finais consumidos e auditados pelo notebook principal.
- `textos_contexto/` reune os pareceres, auditorias e materiais de contexto que explicam a evolucao metodologica e operacional do projeto.

## Observação Sobre Arquivos Grandes

Por restrições de versionamento no GitHub, nem todos os artefatos pesados do projeto ficam armazenados diretamente no repositório. Arquivos como `*.csv`, `*.parquet` e outras saídas volumosas podem existir apenas no ambiente local.
