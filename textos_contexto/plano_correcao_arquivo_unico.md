# Plano de Correcao Minimo para Arquitetura de Arquivo Unico

## Objetivo

Corrigir o projeto para que `notebooks/PAD_projeto.ipynb` se torne o **unico artefato executavel e canonicamente suficiente** do repositorio, sem depender de notebooks modulares, scripts locais removidos ou builders auxiliares.

Este plano parte do parecer registrado em:

- `textos_contexto/auditoria_vulnerabilidades_arquivo_unico.md`

O foco e apenas eliminar as vulnerabilidades que impedem a autocontencao funcional do notebook unico.

## Veredito operacional de partida

O estado atual ainda falha em dois pontos criticos:

1. A primeira celula executavel importa `pad_avaliacao_02_pipeline`, modulo inexistente no working tree.
2. A secao de validacao exige `notebooks/pad_avaliacao_02.ipynb` como arquivo de origem, contrariando a propria ideia de arquivo unico.

Ha ainda um problema documental relevante:

3. O `README.md` continua descrevendo uma arquitetura modular extinta.

## Escopo de correcao

Esta correcao deve:

- preservar `PAD_projeto.ipynb` como volume principal;
- manter o projeto estritamente em formato `.ipynb` como artefato central;
- remover dependencias vivas a `.py` e `.ipynb` auxiliares ausentes;
- alinhar a documentacao publica ao novo contrato do repositorio.

Esta correcao nao precisa:

- restaurar notebooks modulares historicos;
- recriar builders;
- reconstruir a antiga arquitetura de composicao.

## Plano de implementacao

### 1. Internalizar a pipeline no `PAD_projeto.ipynb`

Substituir a dependencia externa `from pad_avaliacao_02_pipeline import ...` por codigo interno no proprio notebook.

O notebook final deve passar a conter, em celulas de codigo proprias:

- as constantes usadas pela analise, inclusive `REGIME_VALUES`;
- as funcoes auxiliares de visualizacao necessarias, inclusive `plotar_barras`;
- a definicao de `run_pipeline`;
- quaisquer helpers estritamente necessarios ao fluxo principal.

Regra de implementacao:

- o codigo deve ser incorporado de forma integral ao notebook final;
- nao pode restar import local para modulo do proprio projeto fora do notebook;
- imports de bibliotecas instaladas continuam permitidos.

Resultado esperado:

- a primeira celula executavel passa a depender apenas de bibliotecas do ambiente e de definicoes presentes no proprio `.ipynb`.

### 2. Reescrever a secao de validacao para operar sem notebook-fonte

Eliminar a verificacao de existencia de `notebooks/pad_avaliacao_02.ipynb`.

A secao de validacao deve trabalhar somente com:

- `dados/saidas_finais/master_municipios_longo.csv`;
- `dados/saidas_finais/master_municipios_analitico_snapshot.csv`;
- opcionalmente o proprio `PAD_projeto.ipynb`, se for necessario inspecionar metadata local do volume.

Regra de implementacao:

- remover `SOURCE_NOTEBOOK_PATH` e qualquer `FileNotFoundError` associado a notebook externo;
- manter as validacoes dos CSVs;
- se houver texto dizendo "Notebook de origem", reescrever para algo compatvel com arquivo unico, por exemplo "Volume principal auditado".

Resultado esperado:

- a secao de validacao continua util, mas sem pressupor a existencia de fontes modulares extintas.

### 3. Converter o notebook em fonte unica real, nao apenas em consolidado visual

Revisar todo o `PAD_projeto.ipynb` para localizar referencias residuais a:

- `pad_avaliacao_02_pipeline`;
- `pad_avaliacao_02.ipynb`;
- `pad_avaliacao_02_dicionario_validacao.ipynb`;
- `benchmarks.ipynb`;
- `build_pad_projeto_notebook.py`;
- qualquer outra expressao que apresente o notebook como artefato gerado por fontes externas inexistentes.

Regra de implementacao:

- referencias historicas podem permanecer apenas se estiverem claramente marcadas como contexto passado;
- nenhuma referencia residual pode ser operacional, isto e, usada por codigo, validacao ou instrucao de uso atual.

Resultado esperado:

- o notebook deixa de carregar dependencias invisiveis da arquitetura modular abandonada.

### 4. Reescrever o `README.md` sob o contrato de arquivo unico

Atualizar o `README.md` para refletir que:

- `notebooks/PAD_projeto.ipynb` e o unico volume principal de execucao e leitura;
- os CSVs em `dados/saidas_finais/` sao os artefatos finais centrais;
- qualquer referencia a arquitetura modular anterior deve ser removida ou explicitamente classificada como historica;
- nao existe builder necessario para uso do projeto no estado corrigido.

O README deve responder de forma simples:

- onde comecar;
- o que o notebook principal faz;
- quais arquivos de dados finais existem;
- quais dependencias do ambiente sao necessarias para abrir e executar o notebook.

Resultado esperado:

- um usuario novo nao sera mais induzido a procurar arquivos extintos.

### 5. Validar a autocontencao do notebook corrigido

Depois da correcao, executar uma verificacao minima orientada a falhas:

- abrir `notebooks/PAD_projeto.ipynb` via `nbformat`;
- confirmar que a primeira celula executavel nao importa modulo local ausente;
- confirmar que nao existe mais `SOURCE_NOTEBOOK_PATH` apontando para notebook externo;
- executar o notebook ou pelo menos os blocos criticos para garantir que o fluxo inicial e a secao de validacao funcionam no working tree atual;
- confirmar que os CSVs finais continuam acessiveis.

## Interfaces e contratos apos a correcao

O contrato publico final deve ficar assim:

- artefato canonico: `notebooks/PAD_projeto.ipynb`;
- artefatos finais de dados: arquivos em `dados/saidas_finais/`;
- sem dependencia obrigatoria de notebooks auxiliares;
- sem dependencia obrigatoria de scripts locais do projeto fora do notebook.

Bibliotecas de terceiros continuam sendo dependencias legitimas do ambiente. O que deve desaparecer e apenas a dependencia a codigo local removido.

## Criterios de aceite

A correcao sera considerada concluida quando todos os pontos abaixo forem verdadeiros:

1. `PAD_projeto.ipynb` abre sem erro de formato.
2. A primeira celula executavel nao contem `from pad_avaliacao_02_pipeline import ...`.
3. O notebook possui internamente as definicoes necessarias para executar a pipeline principal.
4. A secao de validacao nao exige `notebooks/pad_avaliacao_02.ipynb` nem outro notebook auxiliar.
5. O `README.md` nao orienta o usuario para notebooks modulares, builders ou pipeline local ausentes.
6. O notebook passa a ser coerente com a frase: "todo o codigo necessario do projeto esta centralizado em `PAD_projeto.ipynb`".

## Ordem recomendada de execucao

1. Corrigir o codigo do notebook.
2. Corrigir a secao de validacao do notebook.
3. Limpar referencias residuais no notebook.
4. Atualizar o `README.md`.
5. Executar a verificacao final de autocontencao.

## Risco principal de implementacao

O maior risco nao esta na remocao de arquivos modulares, mas em internalizar a pipeline de modo incompleto, deixando funcoes auxiliares, constantes ou transformacoes espalhadas fora do notebook.

Por isso, a implementacao deve ser guiada por esta regra simples:

> Se `PAD_projeto.ipynb` ainda precisar de algum arquivo de codigo local do repositorio para rodar, a correcao nao terminou.
