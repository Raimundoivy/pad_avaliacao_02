# Recomendações para Reconfiguração do Escopo e Tratamento das Lacunas Empíricas

## 1. Finalidade do documento

Este arquivo sistematiza as recomendações formuladas ao longo da revisão recente do projeto, com o objetivo de orientar uma etapa posterior de implementação.

O foco principal é responder a um problema empírico já identificado no `master_municipios_analitico_snapshot.csv`: a presença de `3.960` municípios classificados como `dados_insuficientes`, em um universo total de `5.570` observações municipais.

Os dados já inspecionados sugerem que essa insuficiência não decorre de falhas difusas em toda a base, mas de uma concentração muito forte em um bloco específico da mensuração:

- `3.958` dos `3.960` casos apresentam `taxa_abandono_rural_2024` ausente;
- `3.957` desses `3.958` casos têm `matriculas_ensino_medio_rural_2024 = 0`;
- `600` casos ainda apresentam ausência em `num_tratores_2017`, mas essa lacuna é secundária em relação ao bloqueio educacional.

Em termos práticos, isso significa que a restrição empírica dominante do projeto está concentrada na observabilidade da dimensão educacional rural, e não na integridade geral das dimensões agropecuária e demográfica.

## 2. Diagnóstico técnico consolidado

As evidências reunidas sustentam cinco conclusões centrais:

1. A categoria `dados_insuficientes` está inflada por uma restrição concentrada, e não por ausência generalizada de informação.
2. A ausência de `taxa_abandono_rural_2024` é, na imensa maioria dos casos, estruturalmente associada à inexistência de matrículas rurais no ensino médio, o que sugere problema de aplicabilidade do indicador, e não mera falha de preenchimento.
3. O projeto continua robusto para leituras territoriais ligadas à demografia e à intensificação agropecuária.
4. A dimensão educacional rural, tal como está operacionalizada hoje, não possui observabilidade suficientemente homogênea para sustentar uma tipologia universal do `regime_territorial`.
5. A resposta metodologicamente mais sólida não é imputar dados de forma indiscriminada, mas reconfigurar o escopo analítico e a lógica de classificação.

## 3. Parecer técnico geral

O parecer técnico é favorável à reconfiguração do escopo do projeto.

Essa reconfiguração não deve ser entendida como recuo analítico, mas como ajuste de integridade científica. O projeto permanece consistente como estudo descritivo, comparativo e exploratório, desde que deixe explícito que a mensurabilidade das dimensões não é simétrica em todo o universo municipal.

Em outras palavras:

- o projeto pode continuar afirmando com boa segurança padrões territoriais de intensificação agropecuária e esvaziamento demográfico;
- o projeto deve moderar a universalidade de suas afirmações sobre fragilidade educacional rural;
- a tipologia final precisa refletir melhor a diferença entre `fenômeno não observado`, `fenômeno não aplicável` e `fenômeno parcialmente observado`.

## 4. Recomendações metodológicas prioritárias

### 4.1 Reconfigurar o escopo analítico antes de expandir as bases

Antes de buscar novos repositórios informacionais, recomenda-se reespecificar o escopo do projeto com base na observabilidade efetiva já disponível.

Diretriz:

- redefinir o projeto como análise territorial com observabilidade desigual entre dimensões;
- tratar a dimensão educacional rural como eixo condicionado à disponibilidade empírica;
- evitar que a ausência de um único indicador inviabilize a classificação territorial inteira.

Resultado esperado:

- o projeto permanece válido mesmo sem resolver imediatamente a lacuna educacional em todos os municípios.

### 4.2 Separar `NA estrutural` de `NA por falha`

A ausência de `taxa_abandono_rural_2024` não deve ser tratada de maneira uniforme.

Recomendação:

- quando `matriculas_ensino_medio_rural_2024 == 0`, classificar a ausência de abandono rural como **não aplicável** ou **observação estruturalmente indisponível**;
- reservar a categoria de falha ou inconsistência para casos residuais em que haja matrícula positiva, mas abandono ausente.

Resultado esperado:

- maior precisão conceitual na interpretação das lacunas;
- redução de falsos casos de `dados_insuficientes`.

### 4.3 Evitar imputação automática de abandono rural

Não se recomenda preencher `taxa_abandono_rural_2024` com zero ou com média regional de forma mecânica.

Justificativa:

- ausência de base para cálculo não equivale a abandono zero;
- a imputação indiscriminada produziria viés substantivo e reduziria a validade do constructo educacional.

Diretriz:

- manter `taxa_abandono_rural_2024` como ausente quando o indicador não for aplicável;
- explicitar essa condição por meio de flags e categorias de completude.

### 4.4 Revisar a lógica de `regime_territorial`

A regra atual deve ser revista para impedir que uma única dimensão bloqueie toda a classificação.

Recomendação:

- permitir classificação territorial com base nos sinais efetivamente observáveis;
- registrar a dimensão educacional como `NA estrutural` quando aplicável;
- reservar `dados_insuficientes` para casos de insuficiência analítica real, e não apenas de ausência estrutural de um denominador educacional.

Direção sugerida:

- manter `sinal_intensificacao_agropecuaria` e `sinal_esvaziamento_demografico` como eixos principais;
- rebaixar `sinal_fragilidade_educacional` a eixo condicional quando a base educacional rural for inexistente;
- considerar uma tipologia mais explícita de insuficiência.

### 4.5 Criar tipologia de incompletude

Em vez de manter uma única rubrica residual, recomenda-se decompor a insuficiência observacional.

Exemplos possíveis:

- `insuficiencia_educacional_estrutural`
- `insuficiencia_educacional_inconsistente`
- `insuficiencia_mecanizacao`
- `insuficiencia_demografica_residual`

Resultado esperado:

- ganho de transparência analítica;
- melhora na leitura metodológica e na interpretação final dos artefatos.

## 5. Recomendações sobre dados e novos repositórios informacionais

### 5.1 Expandir bases apenas se a expansão atacar a lacuna central

A agregação de novas fontes faz sentido somente se aumentar de fato a observabilidade da dimensão educacional rural.

Tipos de expansão potencialmente úteis:

- bases complementares sobre oferta educacional rural;
- indicadores adicionais de permanência, evasão, distorção idade-série ou trajetória escolar;
- proxies territorialmente consistentes para acesso à educação rural de nível médio.

Tipos de expansão que devem ser evitados:

- inclusão de novas bases agropecuárias que não alterem o problema central;
- incorporação de fontes com temporalidade incompatível sem ganho analítico claro;
- repositórios que ampliem volume e complexidade sem reduzir a insuficiência empírica do bloco educacional.

### 5.2 Auditar manualmente os casos residuais anômalos

Os casos em que há matrícula rural positiva, mas `taxa_abandono_rural_2024` ausente, devem ser tratados como inconsistência focal.

Recomendação:

- gerar uma lista de exceções;
- revisar a extração ou a fonte original;
- decidir caso a caso se cabe recuperação na fonte, exclusão justificada ou marcação como inconsistente.

## 6. Recomendações de implementação

### 6.1 Novas colunas recomendadas no snapshot

Para sustentar a reconfiguração metodológica, recomenda-se adicionar ao snapshot analítico colunas como:

- `flag_abandono_rural_aplicavel`
- `flag_lacuna_educacional_estrutural`
- `flag_lacuna_tratores`
- `motivo_dados_insuficientes`
- `grau_completude_analitica`

Esses campos devem funcionar como interface explícita entre a etapa de preparação dos dados e a etapa interpretativa.

### 6.2 Regras mínimas de decisão sugeridas

Regras recomendadas para implementação:

1. Se `matriculas_ensino_medio_rural_2024 == 0`, então `flag_abandono_rural_aplicavel = False`.
2. Se `flag_abandono_rural_aplicavel = False` e os demais sinais principais estiverem disponíveis, não classificar automaticamente o município como `dados_insuficientes`.
3. Se houver matrícula rural positiva e abandono ausente, marcar como inconsistência observacional.
4. Se lacunas secundárias permanecerem apenas em mecanização, registrar isso em flag específica antes de recorrer à insuficiência total.

### 6.3 Dupla leitura recomendada

Para robustez metodológica, recomenda-se produzir duas leituras analíticas:

- **Leitura estrita:** sem imputação, com insuficiências registradas explicitamente;
- **Leitura sensível:** com tratamento de aplicabilidade e, se necessário, imputações conservadoras apenas em variáveis secundárias.

Objetivo:

- medir o quanto as conclusões mudam quando as lacunas são tratadas de forma mais ou menos restritiva.

## 7. Implicações científicas esperadas da reconfiguração

Se implementadas, as medidas acima tendem a produzir os seguintes ganhos:

- maior validade interpretativa da tipologia territorial;
- redução da inflação artificial da categoria `dados_insuficientes`;
- maior honestidade metodológica sobre o alcance do projeto;
- melhor distinção entre ausência de fenômeno, ausência de observação e não aplicabilidade;
- maior coerência entre o escopo científico declarado e a mensurabilidade empírica efetivamente disponível.

Ao mesmo tempo, a reconfiguração preserva o núcleo forte do projeto:

- integração territorial por `codigo_municipio`;
- análise comparativa entre grandes regiões e grupos;
- leitura descritiva da intensificação agropecuária;
- articulação entre mudança territorial e limites de observação educacional.

## 8. Ordem recomendada para implementação futura

Sequência sugerida:

1. Reespecificar o escopo metodológico nos textos do projeto.
2. Criar flags de aplicabilidade e completude no snapshot analítico.
3. Revisar a regra de `regime_territorial`.
4. Decompor `dados_insuficientes` por motivo.
5. Auditar os casos residuais inconsistentes.
6. Avaliar, somente depois disso, a conveniência de incorporar novas bases.

## 9. Critério de sucesso

Esta agenda de revisão pode ser considerada bem implementada quando:

- a categoria `dados_insuficientes` deixar de funcionar como residual excessivamente genérico;
- a ausência estrutural de base educacional rural estiver explicitamente modelada;
- o `regime_territorial` não depender universalmente de uma dimensão empiricamente não observável em grande parte dos municípios;
- o texto metodológico do projeto refletir com clareza o novo escopo e seus limites;
- eventual expansão de bases estiver vinculada a um ganho empírico real, e não apenas a acréscimo de volume informacional.
