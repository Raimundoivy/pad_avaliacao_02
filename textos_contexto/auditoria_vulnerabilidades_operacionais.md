# Auditoria Diagnóstica de Vulnerabilidades Operacionais do Projeto

## Objeto auditado

- Repositório local em `/home/raimundoivy/Documents/pad_avaliação_02`
- Foco principal: **execução, reprodutibilidade, entrega e manutenção**
- Fonte de verdade adotada: **estado atual do working tree**
- Commit de referência observado: `a60453d`

## Resumo executivo

**Veredito geral:** o projeto, no estado atual do working tree, apresenta **risco operacional alto** e deve ser considerado **parcialmente entregável, mas não reprodutível nem executável de forma íntegra**.

O repositório ainda entrega um artefato principal visível, `notebooks/PAD_projeto.ipynb`, e preserva os dois CSVs finais em `dados/saidas_finais/`. No entanto, a cadeia operacional que deveria sustentar esse volume está rompida: o notebook principal depende de módulo, notebooks auxiliares e builder que hoje **não existem** no working tree.

Contagem de achados:

- `Crítica`: 2
- `Alta`: 4
- `Média`: 2
- `Baixa`: 1

Riscos mais críticos:

1. O primeiro bloco executável de `PAD_projeto.ipynb` importa `pad_avaliacao_02_pipeline`, mas `notebooks/pad_avaliacao_02_pipeline.py` está ausente e o import falha com `ModuleNotFoundError`.
2. O bloco de validação do volume principal exige `notebooks/pad_avaliacao_02.ipynb` como arquivo de origem e dispara `FileNotFoundError` no estado atual.
3. O `README.md` orienta o usuário a abrir notebooks, pipeline e builders que não existem no working tree.
4. O volume final funciona como um **falso consolidado**: ele parece concentrar o projeto, mas embute código que depende de componentes removidos.

## Evidências de contexto

### Estado observado em disco

O diretório `notebooks/` contém apenas:

- `notebooks/PAD_projeto.ipynb`

Arquivos críticos ausentes:

- `notebooks/pad_avaliacao_02.ipynb`
- `notebooks/pad_avaliacao_02_dicionario_validacao.ipynb`
- `notebooks/pad_avaliacao_02_pipeline.py`
- `notebooks/benchmarks.ipynb`
- `notebooks/build_pad_projeto_notebook.py`

Arquivos finais presentes:

- `dados/saidas_finais/master_municipios_longo.csv`
- `dados/saidas_finais/master_municipios_analitico_snapshot.csv`

### Estado do git durante a auditoria

O working tree estava sujo no momento da inspeção:

- `M notebooks/PAD_projeto.ipynb`
- `D notebooks/benchmarks.ipynb`

Isso significa que a auditoria foi realizada sobre um estado não publicado e potencialmente divergente do último commit.

## Matriz de achados

| ID | Severidade | Categoria | Achado | Evidência | Impacto | Recomendação |
| --- | --- | --- | --- | --- | --- | --- |
| `AOP-01` | `Crítica` | Execução | `PAD_projeto.ipynb` depende de módulo ausente (`pad_avaliacao_02_pipeline`) | `notebooks/PAD_projeto.ipynb:84`; teste de import retornou `ModuleNotFoundError` | O volume principal falha já no primeiro bloco executável | Restaurar ou reincorporar a pipeline no working tree, ou remover a dependência viva do notebook |
| `AOP-02` | `Crítica` | Execução | O bloco de validação exige notebook de origem ausente | `notebooks/PAD_projeto.ipynb:3372-3374` e `notebooks/PAD_projeto.ipynb:3383` | A validação interna do volume principal quebra mesmo com os CSVs finais presentes | Ajustar o código consolidado para não exigir o notebook modular ausente, ou restaurar o arquivo exigido |
| `AOP-03` | `Alta` | Integridade documental | O `README.md` aponta para notebooks, pipeline e builders inexistentes | `README.md:12-17`, `README.md:23`, `README.md:34-38`, `README.md:65-67` | Usuário novo é conduzido a caminhos inválidos e recebe uma descrição incorreta do projeto | Atualizar o README para refletir o estado real do working tree |
| `AOP-04` | `Alta` | Reprodutibilidade | A cadeia declarada de geração do volume principal está quebrada | `README.md:17`; `notebooks/build_pad_projeto_notebook.py` ausente em disco | O artefato principal existe, mas não pode ser regenerado a partir do estado atual | Restaurar o builder ou redefinir a documentação para assumir `PAD_projeto.ipynb` como artefato estático |
| `AOP-05` | `Alta` | Entrega | O projeto opera como **falso consolidado** | `notebooks/PAD_projeto.ipynb:84`, `notebooks/PAD_projeto.ipynb:3372-3374` | O notebook parece autossuficiente, mas depende de peças removidas; a entrega é enganosa do ponto de vista operacional | Tornar o volume realmente autossuficiente ou restabelecer a arquitetura modular |
| `AOP-06` | `Alta` | Manutenção | O benchmark segue referenciado, mas o notebook modular correspondente está ausente | `README.md:15`, `README.md:36`, `README.md:67`; `notebooks/PAD_projeto.ipynb:6029-6035` | A origem modular do apêndice técnico se perde, dificultando manutenção e revisão do benchmark | Restaurar `notebooks/benchmarks.ipynb` ou retirar sua apresentação como componente modular ativo |
| `AOP-07` | `Média` | Governança do working tree | O estado auditado não coincide com um snapshot limpo do repositório | `git status --short` com `M notebooks/PAD_projeto.ipynb` e `D notebooks/benchmarks.ipynb` | A análise operacional pode divergir do histórico compartilhado e dificultar suporte entre colaboradores | Limpar o working tree ou explicitar que a auditoria incide sobre um estado local transitório |
| `AOP-08` | `Média` | Integridade dos artefatos | Os CSVs finais existem, mas a cadeia mínima para reproduzi-los a partir do working tree está rompida | presença de `dados/saidas_finais/*.csv` e ausência de pipeline/notebooks modulares | O projeto preserva resultado, mas perde rastreabilidade operacional de geração | Restabelecer a cadeia reprodutível ou declarar formalmente os CSVs como artefatos congelados |
| `AOP-09` | `Baixa` | Ambiente de execução | O ambiente emite alertas de cache do Matplotlib e `fontconfig` | imports de bibliotecas geraram aviso de diretório não gravável em `~/.config/matplotlib` | Não bloqueia a auditoria, mas produz ruído e pode afetar performance/portabilidade | Definir `MPLCONFIGDIR` para diretório gravável em execuções futuras |

## Achados detalhados por categoria

### 1. Execução e reprodutibilidade

#### `AOP-01` — módulo principal ausente no primeiro bloco executável

O primeiro bloco de código do notebook principal importa:

- `from pad_avaliacao_02_pipeline import REGIME_VALUES, plotar_barras, run_pipeline`

Evidência:

- `notebooks/PAD_projeto.ipynb:84`
- teste direto de import no working tree: `ModuleNotFoundError: No module named 'pad_avaliacao_02_pipeline'`

Interpretação:

- o notebook **abre estruturalmente**;
- mas sua execução falha logo no início;
- portanto, a entrega atual não é operacionalmente íntegra.

#### `AOP-02` — bloco de validação exige notebook de origem ausente

O volume final declara:

- `SOURCE_NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "pad_avaliacao_02.ipynb"`
- em seguida, verifica esse caminho e lança `FileNotFoundError` se ele não existir.

Evidência:

- `notebooks/PAD_projeto.ipynb:3372-3374`
- `notebooks/PAD_projeto.ipynb:3383`

Como `notebooks/pad_avaliacao_02.ipynb` não existe no working tree, a seção de validação é operacionalmente quebrada.

### 2. Integridade documental e de entrega

#### `AOP-03` — README descreve um projeto que não existe mais em disco

O `README.md` direciona o leitor para:

- `notebooks/pad_avaliacao_02.ipynb`
- `notebooks/pad_avaliacao_02_dicionario_validacao.ipynb`
- `notebooks/benchmarks.ipynb`
- `notebooks/build_pad_projeto_notebook.py`
- `notebooks/pad_avaliacao_02_pipeline.py`

Evidência:

- `README.md:12-17`
- `README.md:23`
- `README.md:34-38`

No estado auditado, todos esses caminhos estão ausentes. Isso compromete:

- onboarding;
- auditabilidade;
- entendimento de papéis dos arquivos;
- confiabilidade da entrega.

#### `AOP-04` — cadeia declarada de geração do volume final está indisponível

O README afirma que `PAD_projeto.ipynb` é gerado por `notebooks/build_pad_projeto_notebook.py`, mas esse arquivo não existe no working tree.

Evidência:

- `README.md:17`
- verificação de existência em disco: `notebooks/build_pad_projeto_notebook.py: MISSING`

Impacto:

- o projeto parece reproduzível;
- na prática, o artefato principal não pode ser regenerado a partir do estado atual.

#### `AOP-05` — risco de “falso consolidado”

`PAD_projeto.ipynb` é apresentado como volume principal e consolidado, mas mantém dependências vivas sobre notebooks e pipeline que não existem.

Esse padrão cria um problema específico:

- visualmente, o projeto parece centralizado e autossuficiente;
- operacionalmente, ele não é autossuficiente.

Isso torna a entrega enganosa para qualquer avaliador, docente ou mantenedor que tente executar o notebook.

### 3. Acoplamento e manutenção

#### `AOP-06` — benchmark sem origem modular disponível

O README ainda apresenta `notebooks/benchmarks.ipynb` como notebook modular, e o volume final embute células do benchmark com imports como:

- `pyarrow`
- `fastavro`
- `pyspark`

Evidência:

- `README.md:15`
- `README.md:36`
- `README.md:67`
- `notebooks/PAD_projeto.ipynb:6029-6035`

Observação importante:

- as bibliotecas verificadas no ambiente (`pandera`, `matplotlib`, `pandas`, `pyarrow`, `fastavro`, `pyspark`, `polars`, `psutil`, `seaborn`) estão disponíveis;
- portanto, o problema principal aqui **não é dependência Python ausente**;
- o problema é a **perda do notebook modular de origem**, que enfraquece manutenção, revisão e rastreabilidade do apêndice técnico.

#### `AOP-07` — working tree sujo e divergente do histórico

Durante a auditoria, o repositório não estava em estado limpo:

- `M notebooks/PAD_projeto.ipynb`
- `D notebooks/benchmarks.ipynb`

Impacto:

- o estado operacional observado não está ancorado em um snapshot limpo e compartilhável;
- isso dificulta reproduzir o diagnóstico em outra máquina ou por outro colaborador.

### 4. Integridade de artefatos

#### `AOP-08` — artefatos finais presentes, mas cadeia reprodutível quebrada

Há evidência positiva de que os arquivos finais existem:

- `dados/saidas_finais/master_municipios_longo.csv`
- `dados/saidas_finais/master_municipios_analitico_snapshot.csv`

Também existem:

- `dados/cache/protocolo_cache/`
- `dados/intermediarios/`
- `dados/legado/`
- `benchmarks_outputs/`

Porém, os componentes que o projeto declara como produtores ou validadores desses artefatos não estão disponíveis no working tree. Isso cria um estado clássico de **resultado preservado sem cadeia operacional auditável**.

### 5. Ambiente de execução

#### `AOP-09` — alertas de cache gráfico em ambiente restrito

Os testes de import geraram avisos como:

- diretório padrão do Matplotlib não gravável;
- `fontconfig` sem diretórios de cache graváveis.

Isso não impede a auditoria nem foi causa dos erros críticos, mas é uma fragilidade ambiental real em cenários de execução automatizada.

## Cenários validados

### Cenário 1: leitura guiada pelo README

**Resultado:** falha parcial grave.

Um usuário novo não consegue localizar vários arquivos centrais mencionados no README. A documentação pública, portanto, **induz o uso incorreto do repositório**.

### Cenário 2: abertura do notebook principal

**Resultado:** abertura estrutural bem-sucedida, execução inicial quebrada.

`PAD_projeto.ipynb` abre via `nbformat`, mas o primeiro bloco executável depende de `pad_avaliacao_02_pipeline`, que está ausente.

### Cenário 3: reprodutibilidade mínima

**Resultado:** artefatos finais presentes, cadeia reprodutível ausente.

Os CSVs finais existem, mas os componentes declarados para produzi-los ou validá-los não estão todos disponíveis.

### Cenário 4: manutenção futura

**Resultado:** manutenção enfraquecida.

Um mantenedor consegue ver o resultado final e parte dos dados, mas não consegue reconstruir com segurança a arquitetura modular recente do projeto a partir do working tree atual.

### Cenário 5: consistência de entrega

**Resultado:** entrega visualmente consolidada, operacionalmente incompleta.

O projeto entrega um volume final legível, porém esse volume não é executável de ponta a ponta no estado atual do repositório.

## Conclusão

O projeto, no estado atual do working tree, **não apresenta vulnerabilidade principal no nível dos artefatos finais existentes**, mas sim na **camada operacional que deveria sustentar a execução e a reprodutibilidade da entrega**.

Em termos práticos:

- há um notebook principal;
- há CSVs finais;
- há documentação;
- mas o elo entre esses elementos está rompido.

Por isso, o risco global deve ser classificado como:

**alto para execução e manutenção, médio para leitura estática, e enganoso para entrega se o projeto for apresentado como plenamente reprodutível.**

O diagnóstico final é que o repositório atual entrega um produto **parcialmente consumível como documento**, mas **não confiável como sistema operacional reproduzível**.
