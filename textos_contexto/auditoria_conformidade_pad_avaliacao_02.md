# Auditoria de Conformidade do `pad_avaliacao_02.ipynb`

## Objeto auditado

Artefato avaliado: `notebooks/pad_avaliacao_02.ipynb`

Protocolo de referencia: `textos_contexto/PROJETO DE PESQUISA.md`

Premissa de julgamento: o notebook foi avaliado como artefato autonomo. Requisitos so foram marcados como atendidos quando estavam implementados ou demonstrados no proprio notebook e em suas saidas salvas.

Rotulos usados:

- `Conforme`
- `Parcialmente conforme`
- `Nao conforme`
- `Nao demonstrado no artefato`

## Achados principais

1. O notebook atende bem ao papel de pipeline ETL reprodutivel, mas nao entrega o ciclo analitico completo exigido no protocolo. O artefato para na construcao e exportacao de bases, sem estatistica descritiva, sem comparacoes estruturadas e sem visualizacoes.
2. A cobertura substantiva das bases e variaveis esta incompleta. Ha populacao, agricultura, tratores, estabelecimentos e matriculas rurais, mas nao ha evidencia de PPM/rebanhos nem de abandono escolar rural.
3. A regra metodologica de integracao municipal por codigo IBGE de 7 digitos nao foi plenamente cumprida. A base granular exportada contem 2.418 linhas com codigos nao municipais e valores unicos `1`, `2`, `3`, `4` e `5`.
4. A integridade da tabela granular tambem e fraca. O arquivo `dados/legado/master_municipios.csv` possui 2.126.142 repeticoes na chave composta `codigo_municipio + ano_referencia + produto_agricola + dependencia_administrativa`.
5. O recorte educacional rural foi preservado apenas de forma parcial: o notebook usa a planilha e colunas rurais, mas extrai somente matriculas de ensino medio rural, sem abandono escolar rural.

## Matriz de conformidade

| Bloco | Exigencia do protocolo | Evidencia no artefato | Veredito | Impacto |
| --- | --- | --- | --- | --- |
| Escopo metodologico | O projeto deve produzir base integrada e diagnostico analitico-descritivo com ETL, estatistica descritiva, comparacao estruturada e visualizacao; sem ML e sem series temporais formais. | O notebook se apresenta como "ETL PAD - Orquestracao Refatorada" e sua estrutura vai de configuracao a exportacao. Nao ha chamadas de analise estatistica ou visualizacao nas celulas de codigo; as saidas salvas mostram apenas checkpoints, validacoes e `head()` das bases. | `Parcialmente conforme` | Respeita as delimitacoes negativas do protocolo, mas nao entrega a parte analitica positiva que era obrigatoria. |
| Cobertura das bases e variaveis | Devem entrar Populacao Municipal, PAM, PPM, Censo Agropecuario e Censo Escolar; variaveis devem cobrir area colhida, efetivo pecuario, mecanizacao, estrutura fundiaria, populacao total, matriculas rurais e abandono escolar rural. | `RAW_INPUTS` declara apenas educacao, tratores, estabelecimentos, agricultura e populacao. Nao ha entrada de PPM/rebanhos. A ETL da educacao produz apenas `matriculas_ensino_medio_rural`. | `Nao conforme` | A base final nao cobre toda a dimensao produtiva nem todo o bloco educacional prometido pelo protocolo. |
| Regra de integracao municipal | Todas as juncoes devem usar exclusivamente o codigo IBGE de 7 digitos; nomes de municipios nao podem ser criterio de integracao. | As juncoes finais usam `codigo_municipio` e `ano_referencia`, mas a conciliacao interna de tratores usa tambem `nome_localidade`. Alem disso, `dados/legado/master_municipios.csv` contem 2.418 linhas com codigos de tamanho diferente de 7 e unicos `1` a `5`. | `Nao conforme` | Ha violacao direta de uma diretriz metodologica expressa do protocolo, com risco para a validade territorial da base. |
| Filtro rural no educacional | O tratamento do Censo Escolar deve respeitar a localidade rural. | A ETL da educacao le a aba `1.26`, seleciona colunas `rural_total`, `rural_federal`, `rural_estadual`, `rural_municipal` e `rural_privado`, e gera apenas `matriculas_ensino_medio_rural`. | `Parcialmente conforme` | O recorte rural foi respeitado, mas o protocolo exigia tambem abandono escolar rural, que nao aparece. |
| Tabela mestre e integridade referencial | Deve haver tabela mestre por municipio e por ano ou marco temporal, com integridade referencial verificada progressivamente. | `df_master_agrupado` sai com 5.570 linhas, 5.570 municipios e zero duplicatas por `codigo_municipio`. Ja `df_master_granular` sai com 4.252.902 linhas, 5.575 codigos unicos e 2.126.142 duplicatas na chave composta `codigo_municipio + ano_referencia + produto_agricola + dependencia_administrativa`. | `Parcialmente conforme` | A tabela agrupada e coerente, mas a granular nao sustenta bem o papel de tabela mestre analitica. |
| Indicadores derivados | Devem ser produzidos indicadores derivados como variacoes absolutas e relativas, participacoes, razoes simples e eventual indicador composto auditavel. | O notebook apenas renomeia, agrega e pivota colunas. Nao calcula variacoes, taxas, participacoes, razoes nem classificacao de atencao territorial. | `Nao demonstrado no artefato` | Fica ausente uma parte central do valor analitico prometido pelo protocolo. |
| Analise descritiva e comparacoes | Devem ser calculadas frequencia, media, mediana, minimo, maximo, amplitude, quartis, comparacoes entre grupos e associacoes simples. | Nao ha `describe`, `corr`, tabelas analiticas por grupo nem celulas com calculos descritivos. As saidas salvas mostram apenas contagem de linhas/colunas, validacoes simples e amostras das bases. | `Nao demonstrado no artefato` | O artefato nao produz o diagnostico analitico-descritivo que o protocolo dizia entregar. |
| Visualizacoes e comunicacao analitica | Devem existir graficos e tabelas comparativas voltados a comunicacao dos padroes identificados. | Nao ha chamadas a `plot`, `hist`, `boxplot`, `scatter` ou biblioteca grafica equivalente. O notebook nao salva nem exibe graficos ou tabelas comparativas substantivas. | `Nao demonstrado no artefato` | Sem visualizacao, a etapa de interpretacao comparativa prevista na metodologia nao se materializa. |
| Reprodutibilidade e documentacao | O processo tecnico deve ser documentado de forma reprodutivel, da obtencao dos dados ate a apresentacao dos achados. | O notebook tem descoberta de raiz do projeto, parametros, contratos de colunas, validacoes de unicidade, checkpoints e exportacao controlada por flags. Porem para na geracao de arquivos finais e nao documenta achados analiticos. | `Parcialmente conforme` | A engenharia do ETL esta bem cuidada, mas a documentacao do artefato termina antes da etapa de resultados. |

## Evidencias detalhadas

### 1. O notebook e um ETL, nao um notebook analitico completo

- A abertura do artefato se define como "ETL PAD - Orquestracao Refatorada".
- A macroestrutura declarada no notebook e: configuracao, helpers, ETLs por fonte, construcao dos datasets finais, validacoes e exportacao.
- As unicas saidas salvas relevantes sao tabelas de configuracao, checkpoints, validacoes finais, comparacao com artefatos existentes e amostras (`head`) das bases exportadas.

Conclusao: isso comprova conformidade com a etapa de ETL, mas nao com a exigencia de analise descritiva, comparacao estruturada e visualizacao prevista no protocolo.

### 2. A cobertura de dados ficou aquem do protocolo

- O protocolo preve o uso conjunto de Populacao Municipal, PAM, PPM, Censo Agropecuario e Censo Escolar.
- O notebook declara cinco insumos: educacao, tratores, estabelecimentos, agricultura e populacao.
- Nao ha insumo, ETL, coluna final ou checkpoint associado a rebanhos, efetivo pecuario ou qualquer artefato identificavel como PPM.
- No bloco educacional, a unica medida extraida e `matriculas_ensino_medio_rural`.

Conclusao: a base resultante cobre apenas parte do desenho substantivo prometido.

### 3. A regra do codigo IBGE de 7 digitos foi violada

- O protocolo determina que todas as juncoes usem exclusivamente o codigo municipal de 7 digitos.
- Na ETL de tratores, a conciliacao entre os dois blocos usa `codigo_municipio`, `nome_localidade`, `condicao_produtor`, `grupos_atividade` e `codigo_filtro`.
- Na base granular exportada, ha 2.418 linhas com codigos nao municipais. Os codigos invalidos observados sao `1`, `2`, `3`, `4` e `5`.

Conclusao: mesmo que a maior parte das juncoes finais use `codigo_municipio`, o artefato final nao preserva integralmente a restricao metodologica expressa.

### 4. A integridade da granularidade exportada e insuficiente

- `dados/legado/master_municipios_agrupado.csv` esta consistente no nivel municipal mais agregado: 5.570 linhas, 5.570 municipios, zero duplicatas por `codigo_municipio`.
- `dados/legado/master_municipios.csv` possui 4.252.902 linhas, 5.575 codigos distintos e 2.126.142 duplicatas na chave composta `codigo_municipio + ano_referencia + produto_agricola + dependencia_administrativa`.
- O problema ja aparece na origem agricola tratada: `dados/intermediarios/tabela1612_cleaned.csv` contem repeticoes no grao `codigo_municipio + ano_referencia + produto_agricola`. Exemplo observado: municipio `1100205`, ano `1974`, produto `Algodao herbaceo (em caroco)` com dois valores de area colhida (`200.0` e `307.0`).

Conclusao: a tabela granular nao esta em um grao analitico suficientemente controlado para sustentar comparacoes confiaveis como "tabela mestre" do projeto.

### 5. O recorte rural do educacional foi respeitado, mas de forma incompleta

- A ETL de educacao le a aba `1.26` e seleciona apenas colunas `rural_*`.
- O resultado final da educacao tem tres colunas: `codigo_municipio`, `dependencia_administrativa` e `matriculas_ensino_medio_rural`.
- Nao ha qualquer variavel de abandono escolar rural, embora o protocolo a exija explicitamente.

Conclusao: ha aderencia parcial ao requisito de especificidade rural, mas nao ao conjunto completo de indicadores escolares previstos.

## Veredito global

O `pad_avaliacao_02.ipynb` apresenta **aderencia parcial, mas insuficiente para conformidade plena** com o protocolo de pesquisa.

Em termos positivos, o artefato demonstra boa organizacao de ETL: parametrizacao, leitura reprodutivel, contratos de colunas, validacoes basicas, consolidacao de artefatos e uma tabela agrupada municipal coerente. Nessa dimensao de engenharia de dados, o notebook e tecnicamente solido.

Em termos de conformidade total com o protocolo, porem, o artefato falha em pontos centrais:

- nao cobre todas as bases e variaveis prometidas;
- nao entrega os indicadores derivados previstos;
- nao realiza analise descritiva nem comparacoes estruturadas;
- nao produz visualizacoes;
- nao preserva integralmente a regra de integracao exclusiva por codigo municipal IBGE de 7 digitos;
- nao oferece um diagnostico interpretativo autonomo, apenas a infraestrutura de dados para que esse diagnostico venha a existir depois.

## Lacunas criticas para conformidade plena

1. Incluir a dimensao PPM/rebanhos e as variaveis de efetivo pecuario.
2. Incluir abandono escolar rural, e nao apenas matriculas rurais.
3. Filtrar estritamente os codigos municipais de 7 digitos antes da consolidacao final.
4. Sanear a duplicidade estrutural da base agricola e da base granular.
5. Implementar indicadores derivados previstos no protocolo.
6. Acrescentar estatistica descritiva, comparacoes territoriais e medidas simples de associacao.
7. Produzir visualizacoes e tabelas comparativas no proprio notebook.
8. Encerrar o artefato com achados analiticos documentados, e nao apenas com exportacao de datasets.

## Referencia sintese

Se o criterio de avaliacao for "qualidade do ETL", o notebook vai bem.

Se o criterio for "conformidade com a totalidade das exigencias do protocolo de pesquisa", o notebook **nao esta conforme**.
