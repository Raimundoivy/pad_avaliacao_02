# Validacao de Consonancia com o PROJETO DE PESQUISA DOIS

## Objeto e protocolo adotado

- Objeto validado: projeto local em `README.md`, `notebooks/pad_avaliacao_02.ipynb`, `notebooks/pad_avaliacao_02_pipeline.py` e artefatos em `dados/saidas_finais/`.
- Protocolo de referencia: `PROJETO DE PESQUISA DOIS.md`.
- Criterio de julgamento: consonancia com o plano revisado, e nao com o escopo causal original.
- Regra de contraste historico: `textos_contexto/auditoria_conformidade_pad_avaliacao_02.md` foi considerado apenas como auditoria de um protocolo anterior.

## Veredito executivo

O projeto local esta **substancialmente conforme** ao `PROJETO DE PESQUISA DOIS`.

No estado atual, a implementacao efetiva ja incorpora o reposicionamento metodologico do plano revisado: a pipeline e o notebook principal operam em chave **descritiva, comparativa e multicausal**, sem aprendizado de maquina, sem previsao e sem inferencia causal forte, e o produto sintetico final e uma tipologia explicita de `regime_territorial`.

As exigencias centrais do plano revisado estao materializadas nos artefatos finais:

- base longa auditavel com `4.036.741 x 21`;
- snapshot analitico com `5.570 x 39`;
- integracao municipal por `codigo_municipio` com 7 digitos;
- inclusao de PAM, PPM, Censo Agropecuario, populacao municipal e bloco educacional do INEP;
- presenca de `matriculas_ensino_medio_rural_2024`, `taxa_abandono_rural_2024`, `efetivo_bovino_*` e `regime_territorial`;
- estatistica descritiva, comparacoes por quartil, regiao e porte, correlacao simples, visualizacoes e conclusao interpretativa.

As principais ressalvas residuais nao descaracterizam o alinhamento central, mas merecem registro:

1. A documentacao de alto nivel ainda esta parcialmente desalinhada, porque o `README.md` permanece centrado no historico de benchmarking e nao no projeto territorial revisado.
2. A cobertura de alguns blocos e desigual no tempo e na disponibilidade analitica: a camada `ppm_rebanhos` aparece de `2010` a `2024`, o `censo_agro` aparece apenas em `2017`, e `taxa_abandono_rural_2024` esta preenchida para `1.612` municipios.
3. O snapshot classifica `3.960` municipios como `dados_insuficientes`, o que limita a abrangencia substantiva da tipologia final, embora nao invalide a aderencia metodologica.

Em sintese: o projeto local **nao reproduz mais as lacunas centrais apontadas pela auditoria antiga** e hoje apresenta aderencia forte ao plano revisado, com pendencias predominantemente documentais e de cobertura dos dados.

## Evidencias consolidadas

### Nivel 1. Documentacao do projeto local

- O `README.md` reconhece os artefatos finais atuais, o schema `4.036.741 x 21` e `5.570 x 39`, a integracao por codigo IBGE de 7 digitos e a tipologia `regime_territorial`, explicitamente alinhada ao `PROJETO DE PESQUISA DOIS`.
- O notebook principal abre declarando o enquadramento por "Regimes Territoriais de Transformacao" e informa que o objetivo e descritivo e comparativo.

### Nivel 2. Implementacao efetiva

- A pipeline define `LONG_SCHEMA` com 21 colunas e construcao de `snapshot_analitico` com agregacao tardia derivada da base longa.
- Ha carregadores especificos para PAM, PPM, Censo Agropecuario, matriculas do INEP e rendimento escolar.
- O snapshot deriva explicitamente `efetivo_bovino_2010`, `efetivo_bovino_2024`, `matriculas_ensino_medio_rural_2024`, `taxa_abandono_rural_2024` e `regime_territorial`.
- A camada analitica inclui estatistica descritiva, comparacoes por quartis, regiao e porte, resumo por regimes e matriz de correlacao.

### Nivel 3. Artefatos finais e execucao local

- `dados/saidas_finais/master_municipios_longo.csv` contem as 21 colunas esperadas.
- `dados/saidas_finais/master_municipios_analitico_snapshot.csv` contem as 39 colunas esperadas.
- Todos os `codigo_municipio` do snapshot possuem 7 digitos.
- A execucao local de `run_pipeline(export=False)` retornou `VALIDACOES_FALSE = []`.

Resumo da execucao local:

| Metrica | Resultado |
| --- | --- |
| `BASE_LONGA_ROWS` | `4.036.741` |
| `BASE_LONGA_COLS` | `21` |
| `SNAPSHOT_ROWS` | `5.570` |
| `SNAPSHOT_COLS` | `39` |
| `VALIDACOES_FALSE` | `[]` |

Resumo dos checkpoints da pipeline:

| Dataset | Linhas | Faixa temporal observada |
| --- | ---: | --- |
| `populacao` | 105.798 | 2006-2024 |
| `pam` | 154.954 | 2006-2024 |
| `ppm` | 11.134 | 2010-2024 |
| `censo_agro` | 16.689 | 2017-2017 |
| `matriculas` | 61.270 | 2024-2024 |
| `matriculas_cor_raca` | 83.550 | 2024-2024 |
| `matriculas_tempo_jornada` | 61.270 | 2024-2024 |
| `rendimento` | 3.542.076 | 2024-2024 |

Distribuicao sintetica dos regimes observados:

| Regime territorial | Municipios | Participacao |
| --- | ---: | ---: |
| `intensificacao_com_esvaziamento_e_fragilidade` | 25 | 0,45% |
| `intensificacao_com_fragilidade_educacional` | 58 | 1,04% |
| `intensificacao_com_adaptacao_relativa` | 261 | 4,69% |
| `baixa_pressao_territorial` | 1.266 | 22,73% |
| `dados_insuficientes` | 3.960 | 71,10% |

## Matriz de conformidade

| Bloco | Exigencia do PROJETO DE PESQUISA DOIS | Evidencia observada | Veredito | Impacto |
| --- | --- | --- | --- | --- |
| Enquadramento metodologico | Analise quantitativa, aplicada, descritiva e exploratoria, sem ML, sem previsao, sem inferencia causal forte, com heuristica de regimes territoriais | O notebook principal se declara orientado por `regime_territorial`; a pipeline conclui com limites metodologicos explicitos: sem ML, sem previsao, sem inferencia causal, com uso descritivo e comparativo | `Conforme` | O projeto atual incorpora corretamente a reorientacao metodologica do plano revisado |
| Cobertura substantiva das bases | Articular populacao municipal, PAM, PPM, Censo Agropecuario e Censo Escolar/INEP | `construir_base_longa()` concatena `populacao`, `pam`, `ppm`, `censo_agro`, `matriculas` e `rendimento`; as validacoes internas exigem cobertura minima desses subdominios | `Conforme` | A espinha dorsal empirica do plano revisado esta presente no fluxo atual |
| Cobertura das variaveis centrais | Cobrir area colhida, efetivo pecuario, mecanizacao, estrutura fundiaria, populacao total, matriculas rurais e abandono rural | O snapshot contem `area_*`, `efetivo_bovino_*`, `num_tratores_2017`, `area_total_agricola_hectares_2017`, `populacao_total_*`, `matriculas_ensino_medio_rural_2024` e `taxa_abandono_rural_2024` | `Conforme` | As variaveis centrais deixaram de ser uma lacuna estrutural |
| Regra de integracao territorial | Integracao por codigo IBGE de 7 digitos, vedando cruzamentos textuais como criterio final de juncao | `filtrar_municipios_validos()` padroniza e filtra codigos de 7 digitos; as validacoes internas exigem `base_longa_codigos_com_7_digitos` e `snapshot_codigos_com_7_digitos`; o snapshot exportado passou integralmente nessa checagem | `Conforme` | A principal fragilidade territorial da auditoria antiga foi sanada nos artefatos finais |
| Filtro rural no bloco educacional | Tratar especificamente a localidade rural no bloco educacional | O snapshot extrai `matriculas` com `localizacao="Rural"` e `dependencia_administrativa="Total"`; o abandono e extraido de `rendimento_escolar` com `localizacao="Rural"` e `etapa_ensino="ensino_medio"` | `Conforme` | O recorte educacional rural foi preservado tanto para matriculas quanto para abandono |
| Entrega analitica prevista | Estatistica descritiva, comparacoes estruturadas, visualizacoes e interpretacao por regimes | A pipeline constroi `tabela_descritiva`, `comparacao_quartis`, `comparacao_regiao`, `comparacao_porte`, `resumo_regimes` e `matriz_correlacao`; o notebook exibe secoes de descritiva, graficos e conclusao interpretativa | `Conforme` | O projeto deixou de ser apenas ETL e passou a entregar o diagnostico analitico requerido |
| Reprodutibilidade tecnica | Processo reprodutivel, auditavel, com camada longa, snapshot e validacoes | `run_pipeline()` organiza checkpoints, validacoes, revisao amostral, exportacao e tabelas analiticas; a execucao local sem exportacao retornou `VALIDACOES_FALSE = []` | `Conforme` | A trilha tecnica entre insumos, transformacao e saidas e auditavel |
| Organizacao e documentacao de alto nivel | Repositorio com rastreabilidade clara entre dados, scripts, saidas e documentacao | A estrutura `dados/cache`, `dados/intermediarios`, `dados/legado` e `dados/saidas_finais` preserva rastreabilidade minima, mas o `README.md` ainda privilegia o eixo de benchmark e nao reflete integralmente o projeto territorial revisado | `Parcialmente conforme` | Ha coerencia operacional, mas a narrativa principal do repositorio ainda nao representa plenamente o escopo revisado |
| Cobertura temporal do desenho | Preservar serie longa desde 2006 e trabalhar com anos ou marcos de referencia comparaveis | `populacao` e `pam` cobrem `2006-2024`, mas `ppm` comeca em `2010` e `censo_agro` aparece apenas em `2017`; isso reduz a simetria temporal entre os blocos | `Parcialmente conforme` | Nao invalida o projeto, mas limita a leitura comparativa integral sugerida no plano |

## Checagens explicitas requeridas

| Checagem | Resultado |
| --- | --- |
| A pipeline executa sem falhas e retorna `VALIDACOES_FALSE = []` | `Atendido` |
| `master_municipios_longo.csv` contem `pam_area_colhida`, `ppm_rebanhos`, `censo_agro_estrutura`, `censo_agro_mecanizacao`, `matriculas_ensino_medio` e `rendimento_escolar` | `Atendido` |
| `master_municipios_analitico_snapshot.csv` contem `taxa_abandono_rural_2024`, `matriculas_ensino_medio_rural_2024`, `efetivo_bovino_*` e `regime_territorial` | `Atendido` |
| Todos os `codigo_municipio` dos artefatos finais possuem 7 digitos | `Atendido` |
| O notebook principal exibe secoes de descritiva, comparacoes, graficos e conclusao interpretativa | `Atendido` |

## Classificacao das nao conformidades residuais

### Lacuna metodologica real

- Nao foi observada uma lacuna metodologica central que descaracterize o alinhamento ao `PROJETO DE PESQUISA DOIS`.

### Lacuna documental

- O `README.md` ainda descreve o repositorio prioritariamente como trabalho sobre formatos, serializacao e benchmark, enquanto o projeto principal validado ja opera como estudo territorial aplicado.
- A auditoria antiga em `textos_contexto/auditoria_conformidade_pad_avaliacao_02.md` continua util como historico, mas pode induzir leitura desatualizada se for tomada como parecer vigente.

### Limitacao de cobertura dos dados

- O subdominio `ppm_rebanhos` nao aparece desde 2006 na camada atualmente operacional; o checkpoint observado comeca em `2010`.
- O `censo_agro` aparece apenas em `2017`, embora o desenho conceitual remeta a comparacao entre marcos.
- `taxa_abandono_rural_2024` esta preenchida para `1.612` municipios, o que amplia o grupo `dados_insuficientes`.
- `variacao_populacao_2010_2024_pct` esta preenchida para `5.565` municipios, deixando uma pequena franja sem comparacao demografica completa.

## Conclusao final

Considerando o protocolo correto, o projeto local deve ser avaliado como **conforme em seus eixos centrais e parcialmente conforme apenas em aspectos documentais e de cobertura temporal/dados**.

O achado principal desta validacao e que o repositorio **ja incorporou a reformulacao metodologica do PROJETO DE PESQUISA DOIS** e superou as falhas mais graves que apareciam na auditoria baseada no protocolo anterior. O que permanece em aberto hoje nao e uma quebra do desenho revisado, mas sim:

- necessidade de atualizar a narrativa documental principal do repositorio;
- necessidade de explicitar melhor as limitacoes de cobertura temporal e de disponibilidade do abandono rural;
- necessidade de interpretar com cautela a alta participacao de `dados_insuficientes`.

Se o criterio de avaliacao for a consonancia entre implementacao atual e plano revisado, o parecer final e: **aderencia forte, com ressalvas residuais bem delimitadas**.
