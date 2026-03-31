# Auditoria Diagnostica de Vulnerabilidades sob Arquitetura de Arquivo Unico

## 1. Objeto auditado e premissa adotada

Este parecer reavalia o estado atual do repositrio `pad_avaliacao_02` sob uma premissa diferente da auditoria anterior:

- o artefato canonico do projeto e apenas `notebooks/PAD_projeto.ipynb`;
- a ausencia de notebooks modulares, builders e scripts auxiliares nao conta, por si so, como falha;
- o criterio central passa a ser: o repositorio entrega ou nao um **notebook unico executavel**, autocontido, sem dependencias locais removidas.

O working tree local continua sendo a fonte de verdade. Portanto, a diferenca entre este parecer e `textos_contexto/auditoria_vulnerabilidades_operacionais.md` decorre de **mudanca de criterio**, nao de mudanca do estado auditado.

Veredito geral: **o repositorio ainda nao atende ao modelo de arquivo unico executavel**. Ele entrega um notebook central valido como artefato de leitura, mas ainda depende de componentes externos que deixaram de existir no working tree.

## 2. Resumo executivo

Contagem de achados nesta revisao:

- `Critica`: 2
- `Alta`: 2
- `Media`: 1
- `Baixa`: 1

Riscos centrais:

1. A primeira celula executavel de `PAD_projeto.ipynb` importa `pad_avaliacao_02_pipeline`, mas esse modulo nao existe no working tree atual.
2. A secao de validacao do mesmo notebook exige `notebooks/pad_avaliacao_02.ipynb` como arquivo de origem, o que contradiz a premissa de arquivo unico e falha no estado atual.
3. O `README.md` continua descrevendo uma arquitetura modular extinta, orientando o usuario para arquivos ausentes.
4. O repositorio apresenta `PAD_projeto.ipynb` como volume principal, mas o volume ainda nao e operacionalmente autocontido; trata-se de um arquivo central com dependencias invisiveis.

## 3. Mudanca de criterio em relacao a auditoria anterior

A auditoria anterior perguntava: "o repositorio preserva a integridade operacional da arquitetura modular descrita publicamente?"

Esta auditoria pergunta: "se a arquitetura correta agora e um unico `.ipynb`, o projeto realmente se sustenta como notebook unico executavel?"

Com isso:

- a ausencia de `benchmarks.ipynb`, `build_pad_projeto_notebook.py` e outros notebooks modulares **nao e mais vulnerabilidade autonoma**;
- essas ausencias so contam quando o `README.md` ou o `PAD_projeto.ipynb` ainda dependem delas como parte do contrato de uso;
- as falhas realmente graves passam a ser apenas as que impedem a autocontencao do arquivo unico.

## 4. Matriz de achados reclassificada

| ID | Severidade | Categoria | Achado | Evidencia | Impacto | Recomendacao |
| --- | --- | --- | --- | --- | --- | --- |
| `AUN-01` | `Critica` | Execucao | O notebook unico falha ja na primeira celula executavel por depender de modulo local inexistente | `notebooks/PAD_projeto.ipynb` primeira celula de codigo: `from pad_avaliacao_02_pipeline import REGIME_VALUES, plotar_barras, run_pipeline`; teste direto de import retornou `ModuleNotFoundError` | O artefato central nao consegue inicializar seu fluxo principal sozinho | Reincorporar toda a logica da pipeline dentro do proprio `.ipynb` ou remover a dependencia e substituir por codigo interno equivalente |
| `AUN-02` | `Critica` | Autonomia estrutural | A secao de validacao exige um notebook-fonte removido | Celula de validacao define `SOURCE_NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "pad_avaliacao_02.ipynb"` e levanta `FileNotFoundError` se o arquivo nao existir | Mesmo que a primeira celula fosse corrigida, o notebook continua nao sendo autocontido | Reescrever a validacao para operar apenas sobre os CSVs finais e sobre o proprio `PAD_projeto.ipynb`, sem referencia a notebook externo |
| `AUN-03` | `Alta` | Contrato publico | O `README.md` continua instruindo o usuario a usar arquivos modulares ausentes e descreve uma arquitetura que deixou de valer | `README.md:12-17`, `README.md:23`, `README.md:33-38`, `README.md:65-68` | O repositorio promete um fluxo que nao existe mais e induz erro de uso/manutencao | Reescrever o README para assumir explicitamente `PAD_projeto.ipynb` como unico artefato central e retirar referencias operacionais aos componentes extintos |
| `AUN-04` | `Alta` | Consistencia de entrega | O projeto e apresentado como volume unico, mas o notebook ainda carrega dependencias ocultas da arquitetura modular abandonada | Combinacao entre `AUN-01`, `AUN-02` e o discurso do `README.md` | A entrega parece consolidada, mas nao atende ao padrao de notebook unico executavel | Tratar `PAD_projeto.ipynb` como codigo-fonte integral do projeto ou rebaixar explicitamente sua funcao para artefato de leitura |
| `AUN-05` | `Media` | Governanca local | O working tree auditado nao corresponde a um snapshot limpo | `git status --short` retornou `M notebooks/PAD_projeto.ipynb`, `D notebooks/benchmarks.ipynb` e `?? textos_contexto/auditoria_vulnerabilidades_operacionais.md` | A auditoria incide sobre um estado local transitorio, o que pode dificultar suporte e reproducao colaborativa | Registrar o parecer com o commit-base usado na analise e limpar o working tree antes de publicar o diagnostico |
| `AUN-06` | `Baixa` | Ruido operacional | O ambiente de execucao emite warnings de cache grafico, mas isso nao define a falha principal do arquivo unico | Em testes anteriores, `matplotlib` e `fontconfig` emitiram avisos de diretorios nao gravaveis | Pode gerar ruido ou lentidao, mas nao explica a quebra central do notebook | Ajustar variaveis de ambiente de cache apenas depois de resolver a autocontencao funcional do notebook |

## 5. Achados detalhados por categoria

### 5.1 Execucao e reproducibilidade do notebook unico

O cenario de "arquivo unico executavel" falha logo na entrada. O notebook abre estruturalmente via `nbformat`, com `73` celulas e primeira celula de codigo no indice `3`, mas essa primeira celula executavel depende de `pad_avaliacao_02_pipeline`, ausente no working tree.

Trecho auditado:

```python
from pad_avaliacao_02_pipeline import REGIME_VALUES, plotar_barras, run_pipeline
artifacts = run_pipeline(export=True)
```

Teste direto realizado no mesmo working tree:

```text
ModuleNotFoundError: No module named 'pad_avaliacao_02_pipeline'
```

Sob a nova premissa, isso deixa de ser "falta de modularizacao" e passa a ser uma quebra objetiva de autocontencao: o notebook central ainda nao centraliza, de fato, todo o codigo de que precisa.

### 5.2 Autonomia estrutural do volume principal

Ha uma segunda dependencia estrutural indevida na secao de validacao. A celula correspondente define:

```python
SOURCE_NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "pad_avaliacao_02.ipynb"
for caminho in [LONG_PATH, SNAPSHOT_PATH, SOURCE_NOTEBOOK_PATH]:
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {caminho}")
```

Em um projeto de arquivo unico, esse trecho e incompatvel com o proprio contrato arquitetural. O problema aqui nao e apenas a ausencia do notebook modular, mas o fato de o volume final continuar dependendo dele para se legitimar.

### 5.3 Contrato publico do repositorio

O `README.md` ainda descreve uma topologia modular:

- apresenta `pad_avaliacao_02.ipynb`, `pad_avaliacao_02_dicionario_validacao.ipynb` e `benchmarks.ipynb` como componentes ativos;
- afirma que `PAD_projeto.ipynb` e gerado por `build_pad_projeto_notebook.py`;
- aponta `notebooks/pad_avaliacao_02_pipeline.py` como pipeline principal;
- descreve uma relacao funcional entre arquivos que nao existem mais no working tree.

Sob a nova premissa, isso continua sendo vulnerabilidade alta porque o contrato publico do repositorio contradiz a arquitetura declarada de arquivo unico.

### 5.4 Integridade dos artefatos finais

Os artefatos finais principais estao presentes em disco:

- `dados/saidas_finais/master_municipios_longo.csv`
- `dados/saidas_finais/master_municipios_analitico_snapshot.csv`

Tambem existem arquivos adicionais em `dados/saidas_finais/`, como `master_municipios_painel.csv` e `master_municipios_snapshot.csv`.

Isso mostra que o repositorio preserva resultados materiais da analise. No entanto, como o notebook unico declara executar `run_pipeline(export=True)` sem conseguir importar a pipeline, a cadeia de reproducao ainda nao e sustentada pelo proprio `PAD_projeto.ipynb`.

### 5.5 Achados que deixaram de ser vulnerabilidade autonoma

Sob esta revisao, **nao** foram mantidos como achados independentes:

- a mera ausencia de `benchmarks.ipynb`;
- a mera ausencia de `build_pad_projeto_notebook.py`;
- a mera ausencia de notebooks modulares historicos.

Esses pontos so reaparecem indiretamente quando o `README.md` ainda os promete ou quando o `PAD_projeto.ipynb` ainda depende da logica que deveria ter sido internalizada.

## 6. Cenarios validados

### Cenario 1: inicializacao do notebook unico

- `PAD_projeto.ipynb` abre sem erro de formato.
- A primeira celula executavel falha conceitualmente no working tree atual por depender de modulo local ausente.

Resultado: **falha** para o criterio de notebook unico executavel.

### Cenario 2: autonomia estrutural

- O notebook contem referencia obrigatoria a `notebooks/pad_avaliacao_02.ipynb`.
- Essa referencia e usada como requisito de existencia em tempo de execucao.

Resultado: **falha** para o criterio de autocontencao.

### Cenario 3: contrato publico do repositorio

- O `README.md` nao apresenta `PAD_projeto.ipynb` como unico artefato operacional suficiente.
- O documento ainda instrui o usuario a navegar por componentes modulares ausentes.

Resultado: **falha** de consistencia documental.

### Cenario 4: artefatos finais

- Os CSVs finais existem fisicamente.
- O notebook unico, contudo, nao demonstra capacidade autonoma de reproduzi-los no estado atual.

Resultado: **parcial**. Ha entrega material, mas nao reproducao autonoma.

### Cenario 5: consistencia de entrega

- O repositorio entrega um notebook central coerente como volume de leitura.
- O mesmo repositorio nao entrega, ainda, um notebook unico executavel no sentido estrito definido nesta revisao.

Resultado: **falha** para o padrao adotado.

## 7. Conclusao

Sob a premissa oficial de centralizacao integral em um unico arquivo `.ipynb`, a auditoria precisa ser mais simples e mais exigente ao mesmo tempo:

- a inexistencia de modulos auxiliares deixa de ser problema por si;
- qualquer dependencia viva a esses modulos passa a ser problema decisivo.

Com essa regua, o estado atual do projeto ainda nao satisfaz o modelo de **arquivo unico executavel**. O repositorio possui um notebook central legitimo como artefato final de leitura, mas esse notebook ainda conserva dependencias operacionais de uma arquitetura modular que foi abandonada.

Em termos objetivos: **o projeto esta centralizado visualmente, mas ainda nao esta centralizado funcionalmente**.
