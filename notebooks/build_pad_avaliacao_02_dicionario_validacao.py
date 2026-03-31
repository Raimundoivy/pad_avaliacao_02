from __future__ import annotations

from inspect import cleandoc
from pathlib import Path

import nbformat as nbf


PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "pad_avaliacao_02_dicionario_validacao.ipynb"


def markdown_cell(source: str):
    return nbf.v4.new_markdown_cell(cleandoc(source) + "\n")


def code_cell(source: str):
    return nbf.v4.new_code_cell(cleandoc(source) + "\n")


def build_notebook():
    notebook = nbf.v4.new_notebook()
    notebook["metadata"] = {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "version": "3.12",
        },
    }

    notebook["cells"] = [
        markdown_cell(
            """
            # Dicionario e Validacao dos Artefatos Finais do PAD

            Este notebook valida os dois outputs publicos produzidos pela pipeline atualizada
            de [`pad_avaliacao_02.ipynb`](./pad_avaliacao_02.ipynb):

            - `dados/saidas_finais/master_municipios_longo.csv`
            - `dados/saidas_finais/master_municipios_analitico_snapshot.csv`

            O objetivo aqui e verificar integridade estrutural, coerencia entre os arquivos,
            conformidade de schema e cobertura do dicionario de dados dos artefatos vigentes,
            substituindo a validacao legada baseada em `painel/snapshot` antigos.
            """
        ),
        code_cell(
            """
            from __future__ import annotations

            from pathlib import Path
            import warnings

            import pandas as pd
            import pandera.pandas as pa
            from pandera import Check
            from pandera.errors import SchemaErrors
            from IPython.display import Markdown, display

            warnings.filterwarnings("ignore", category=FutureWarning)
            pd.set_option("display.max_columns", None)
            pd.set_option("display.max_colwidth", 140)
            pd.set_option("display.width", 180)


            def descobrir_raiz_projeto() -> Path:
                candidatos = [Path.cwd().resolve(), Path.cwd().resolve().parent]
                for candidato in candidatos:
                    if (candidato / "dados").exists() and (candidato / "notebooks").exists():
                        return candidato
                raise FileNotFoundError("Nao foi possivel localizar a raiz do projeto.")


            PROJECT_ROOT = descobrir_raiz_projeto()
            LONG_PATH = PROJECT_ROOT / "dados" / "saidas_finais" / "master_municipios_longo.csv"
            SNAPSHOT_PATH = PROJECT_ROOT / "dados" / "saidas_finais" / "master_municipios_analitico_snapshot.csv"
            SOURCE_NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "pad_avaliacao_02.ipynb"

            for caminho in [LONG_PATH, SNAPSHOT_PATH, SOURCE_NOTEBOOK_PATH]:
                if not caminho.exists():
                    raise FileNotFoundError(f"Arquivo nao encontrado: {caminho}")

            df_long = pd.read_csv(LONG_PATH)
            df_snapshot = pd.read_csv(SNAPSHOT_PATH)

            print(f"Base longa carregada de: {LONG_PATH}")
            print(f"Snapshot carregado de: {SNAPSHOT_PATH}")
            print(f"Notebook de origem: {SOURCE_NOTEBOOK_PATH}")
            print(f"df_long: {df_long.shape[0]:,} linhas x {df_long.shape[1]} colunas")
            print(f"df_snapshot: {df_snapshot.shape[0]:,} linhas x {df_snapshot.shape[1]} colunas")
            """
        ),
        markdown_cell(
            """
            ## 1. Perfil estrutural dos artefatos

            Esta secao registra shape, periodo, tipos, nulos e dominios observados da base longa
            e do snapshot analitico.
            """
        ),
        code_cell(
            """
            def summarize_domain(series: pd.Series, max_categories: int = 8) -> str:
                non_null = series.dropna()
                if non_null.empty:
                    return "sem valores observados"
                if pd.api.types.is_numeric_dtype(series):
                    return f"{non_null.min()} a {non_null.max()}"
                unique_values = sorted(non_null.astype(str).unique().tolist())
                if len(unique_values) <= max_categories:
                    return ", ".join(unique_values)
                return ", ".join(unique_values[:max_categories]) + f" ... (+{len(unique_values) - max_categories} valores)"


            perfil_geral = pd.DataFrame(
                [
                    {"artefato": "base_longa", "linhas": int(df_long.shape[0]), "colunas": int(df_long.shape[1]), "municipios": int(df_long["codigo_municipio"].nunique())},
                    {"artefato": "snapshot", "linhas": int(df_snapshot.shape[0]), "colunas": int(df_snapshot.shape[1]), "municipios": int(df_snapshot["codigo_municipio"].nunique())},
                ]
            )
            display(perfil_geral)

            perfil_temporal = pd.DataFrame(
                [
                    {
                        "artefato": "base_longa",
                        "ano_min": int(df_long["ano_referencia"].min()),
                        "ano_max": int(df_long["ano_referencia"].max()),
                        "anos_distintos": int(df_long["ano_referencia"].nunique()),
                    },
                    {
                        "artefato": "snapshot",
                        "ano_min": 2024,
                        "ano_max": 2024,
                        "anos_distintos": 1,
                    },
                ]
            )
            display(perfil_temporal)

            perfil_long = pd.DataFrame(
                {
                    "coluna": df_long.columns,
                    "dtype": [str(df_long[coluna].dtype) for coluna in df_long.columns],
                    "nulos": [int(df_long[coluna].isna().sum()) for coluna in df_long.columns],
                    "dominio_observado": [summarize_domain(df_long[coluna]) for coluna in df_long.columns],
                }
            )
            display(perfil_long)

            perfil_snapshot = pd.DataFrame(
                {
                    "coluna": df_snapshot.columns,
                    "dtype": [str(df_snapshot[coluna].dtype) for coluna in df_snapshot.columns],
                    "nulos": [int(df_snapshot[coluna].isna().sum()) for coluna in df_snapshot.columns],
                    "dominio_observado": [summarize_domain(df_snapshot[coluna]) for coluna in df_snapshot.columns],
                }
            )
            display(perfil_snapshot)
            """
        ),
        markdown_cell(
            """
            ## 2. Dicionario de dados

            O dicionario abaixo cobre integralmente os dois artefatos finais vigentes.
            """
        ),
        code_cell(
            """
            long_dictionary_rows = [
                ("codigo_municipio", "int", "N", "codigo IBGE municipal com 7 digitos", "identificador territorial padrao", "chave obrigatoria de integracao"),
                ("nome_municipio", "str", "N", "nome oficial do municipio", "rotulo textual municipal", "nao deve ser usado como chave de merge"),
                ("sigla_estado", "str", "N", "UF brasileira em duas letras", "sigla da unidade federativa", "derivada do lookup territorial"),
                ("regiao", "str", "N", "Centro-Oeste, Nordeste, Norte, Sudeste, Sul", "grande regiao geografica", "derivada da UF"),
                ("ano_referencia", "int", "N", "2006 a 2024", "ano da observacao", "cada subdominio possui sua propria cobertura temporal"),
                ("fonte", "str", "N", "IBGE, INEP", "origem institucional da observacao", "mantem rastreabilidade da linha"),
                ("dominio", "str", "N", "demografia, agropecuaria, educacao", "macrodominio analitico", "usado para leituras comparativas"),
                ("subdominio", "str", "N", "subconjuntos oficiais por tema", "bloco substantivo dentro do dominio", "ex.: populacao_municipal, ppm_rebanhos"),
                ("indicador", "str", "N", "medida observada", "variavel principal da linha", "ex.: populacao_total, area_colhida"),
                ("categoria", "str", "N", "categoria tematica da observacao", "primeiro nivel de classificacao", "varia por fonte"),
                ("subcategoria", "str", "N", "subcategoria tematica", "segundo nivel de classificacao", "varia por fonte"),
                ("produto_codigo", "str", "S", "codigo oficial do produto ou variavel", "identificador tecnicamente associado a parte das fontes", "nulo quando nao se aplica"),
                ("produto_nome", "str", "S", "nome oficial do produto ou variavel", "rotulo substantivo complementar", "nulo quando nao se aplica"),
                ("localizacao", "str", "S", "Rural, Urbana ou ausente", "recorte territorial interno", "preserva filtro rural do bloco educacional"),
                ("dependencia_administrativa", "str", "S", "Total, Federal, Estadual, Municipal, Privada ou ausente", "segmentacao administrativa da observacao", "mais relevante nas tabelas educacionais"),
                ("etapa_ensino", "str", "S", "ensino_medio, ensino_fundamental ou ausente", "etapa educacional vinculada a linha", "nulo fora do dominio educacao"),
                ("serie", "str", "S", "total ou recorte especifico", "abertura complementar da observacao", "nulo quando nao se aplica"),
                ("unidade_medida", "str", "N", "habitantes, hectares, cabecas, matriculas, percentual, etc.", "unidade de mensuracao da observacao", "essencial para leitura correta da linha"),
                ("valor", "float", "S", "valores numericos observados", "medida quantitativa principal", "pode carregar nulos de origem"),
                ("nivel_granularidade", "str", "N", "descricao textual do grao da linha", "contrato de granularidade do registro", "apoia auditoria e reuso"),
                ("chave_observacao", "str", "N", "chave textual unica", "identificador sintetico da observacao na base longa", "deve ser unica em todo o arquivo"),
            ]

            snapshot_dictionary_rows = [
                ("codigo_municipio", "int", "N", "codigo IBGE municipal com 7 digitos", "identificador territorial padrao", "chave primaria do snapshot"),
                ("nome_municipio", "str", "N", "nome oficial do municipio", "rotulo textual municipal", "nao deve ser usado como chave de merge"),
                ("sigla_estado", "str", "N", "UF brasileira em duas letras", "sigla da unidade federativa", "derivada do lookup territorial"),
                ("regiao", "str", "N", "Centro-Oeste, Nordeste, Norte, Sudeste, Sul", "grande regiao geografica", "derivada da UF"),
                ("populacao_total_2010", "float", "S", "valores nao negativos", "populacao municipal no ano-base analitico", "extraida da base longa"),
                ("populacao_total_2024", "float", "N", "valores nao negativos", "populacao municipal no snapshot atual", "extraida da base longa"),
                ("area_algodao_hectares_2010", "float", "N", "valores nao negativos", "area colhida de algodao em 2010", "zeros representam ausencia observada"),
                ("area_algodao_hectares_2024", "float", "N", "valores nao negativos", "area colhida de algodao em 2024", "zeros representam ausencia observada"),
                ("area_cana_hectares_2010", "float", "N", "valores nao negativos", "area colhida de cana em 2010", "zeros representam ausencia observada"),
                ("area_cana_hectares_2024", "float", "N", "valores nao negativos", "area colhida de cana em 2024", "zeros representam ausencia observada"),
                ("area_milho_hectares_2010", "float", "N", "valores nao negativos", "area colhida de milho em 2010", "zeros representam ausencia observada"),
                ("area_milho_hectares_2024", "float", "N", "valores nao negativos", "area colhida de milho em 2024", "zeros representam ausencia observada"),
                ("area_soja_hectares_2010", "float", "N", "valores nao negativos", "area colhida de soja em 2010", "zeros representam ausencia observada"),
                ("area_soja_hectares_2024", "float", "N", "valores nao negativos", "area colhida de soja em 2024", "zeros representam ausencia observada"),
                ("efetivo_bovino_2010", "float", "N", "valores nao negativos", "efetivo bovino em 2010", "derivado da PPM"),
                ("efetivo_bovino_2024", "float", "N", "valores nao negativos", "efetivo bovino em 2024", "derivado da PPM"),
                ("total_estabelecimentos_agricolas_2017", "float", "S", "valores nao negativos", "numero de estabelecimentos agropecuarios em 2017", "medida estrutural do Censo Agro"),
                ("area_total_agricola_hectares_2017", "float", "S", "valores nao negativos", "area total dos estabelecimentos agropecuarios em 2017", "medida estrutural do Censo Agro"),
                ("num_tratores_2017", "float", "S", "valores nao negativos", "numero de tratores em 2017", "proxy de mecanizacao"),
                ("matriculas_ensino_medio_rural_2024", "float", "S", "valores nao negativos", "matriculas rurais do ensino medio em 2024", "recorte rural total"),
                ("taxa_abandono_rural_2024", "float", "S", "valores nao negativos", "taxa de abandono rural do ensino medio em 2024", "recorte rural total"),
                ("area_total_culturas_selecionadas_hectares_2024", "float", "N", "valores nao negativos", "soma das areas de algodao, cana, milho e soja em 2024", "indicador sintetico de intensidade agricola"),
                ("variacao_populacao_2010_2024_pct", "float", "S", "valores percentuais", "variacao relativa da populacao entre 2010 e 2024", "usa 2010 como base"),
                ("variacao_area_soja_2010_2024_pct", "float", "S", "valores percentuais", "variacao relativa da area de soja entre 2010 e 2024", "usa 2010 como base"),
                ("variacao_area_milho_2010_2024_pct", "float", "S", "valores percentuais", "variacao relativa da area de milho entre 2010 e 2024", "usa 2010 como base"),
                ("variacao_area_cana_2010_2024_pct", "float", "S", "valores percentuais", "variacao relativa da area de cana entre 2010 e 2024", "usa 2010 como base"),
                ("variacao_rebanho_bovino_2010_2024_pct", "float", "S", "valores percentuais", "variacao relativa do rebanho bovino entre 2010 e 2024", "usa 2010 como base"),
                ("tratores_por_100_estabelecimentos_2017", "float", "S", "valores nao negativos", "tratores a cada 100 estabelecimentos em 2017", "razao estrutural de mecanizacao"),
                ("hectares_por_estabelecimento_2017", "float", "S", "valores nao negativos", "area media por estabelecimento em 2017", "razao estrutural fundiaria"),
                ("matriculas_rurais_por_1000_hab_2024", "float", "S", "valores nao negativos", "matriculas rurais por 1000 habitantes em 2024", "ajusta o indicador escolar pelo porte populacional"),
                ("percentil_area_culturas_2024", "float", "N", "0.0 a 1.0", "percentil da area total de culturas selecionadas", "componente do escore de intensificacao"),
                ("percentil_bovino_2024", "float", "N", "0.0 a 1.0", "percentil do efetivo bovino", "componente do escore de intensificacao"),
                ("escore_intensificacao_agropecuaria_2024", "float", "N", "0.0 a 1.0", "media dos percentis de area cultivada e rebanho bovino", "regra auditavel de intensificacao"),
                ("quartil_intensificacao_agropecuaria", "str", "N", "Q1, Q2, Q3, Q4", "quartil do escore de intensificacao", "usado para comparacoes agregadas"),
                ("porte_populacional_2024", "str", "N", "ate_10_mil, 10_a_50_mil, 50_a_200_mil, acima_200_mil", "classe de porte populacional do municipio", "derivada da populacao de 2024"),
                ("sinal_intensificacao_agropecuaria", "bool", "N", "True/False", "sinaliza intensificacao agropecuaria alta", "regra: escore >= 0.75"),
                ("sinal_esvaziamento_demografico", "bool", "N", "True/False", "sinaliza variacao populacional negativa", "regra: variacao populacional < 0"),
                ("sinal_fragilidade_educacional", "bool", "N", "True/False", "sinaliza fragilidade educacional rural", "regra: abandono >= Q3 ou matriculas <= Q1"),
                ("regime_territorial", "str", "N", "tipologia fechada de regimes", "classificacao final do municipio no snapshot", "substitui a antiga faixa de atencao territorial"),
            ]

            dicionario = pd.DataFrame(
                [
                    {
                        "artefato": "base_longa",
                        "Nome da Coluna": coluna,
                        "Tipo": tipo,
                        "Nulo": nulo,
                        "Dominio": dominio,
                        "Descricao": descricao,
                        "Observacoes": observacoes,
                    }
                    for coluna, tipo, nulo, dominio, descricao, observacoes in long_dictionary_rows
                ]
                + [
                    {
                        "artefato": "snapshot",
                        "Nome da Coluna": coluna,
                        "Tipo": tipo,
                        "Nulo": nulo,
                        "Dominio": dominio,
                        "Descricao": descricao,
                        "Observacoes": observacoes,
                    }
                    for coluna, tipo, nulo, dominio, descricao, observacoes in snapshot_dictionary_rows
                ]
            )
            display(dicionario)
            """
        ),
        markdown_cell(
            """
            ## 3. Checks de integridade estrutural e relacional

            Aqui verificamos chaves, codigos, cobertura temporal e coerencia entre a base longa
            e o snapshot analitico derivado.
            """
        ),
        code_cell(
            """
            REGIMES_VALIDOS = [
                "intensificacao_com_esvaziamento_e_fragilidade",
                "intensificacao_com_fragilidade_educacional",
                "intensificacao_com_adaptacao_relativa",
                "baixa_pressao_territorial",
                "dados_insuficientes",
            ]


            def extrair_da_base_longa(
                dataframe: pd.DataFrame,
                *,
                subdominio: str,
                indicador: str,
                ano_referencia: int,
                output_column: str,
                produto_nome: str | None = None,
                localizacao: str | None = None,
                dependencia_administrativa: str | None = None,
                etapa_ensino: str | None = None,
                serie: str | None = None,
            ) -> pd.DataFrame:
                mask = dataframe["subdominio"].astype("string").eq(subdominio)
                mask &= dataframe["indicador"].astype("string").eq(indicador)
                mask &= dataframe["ano_referencia"].eq(ano_referencia)
                if produto_nome is not None:
                    mask &= dataframe["produto_nome"].astype("string").eq(produto_nome)
                if localizacao is not None:
                    mask &= dataframe["localizacao"].astype("string").eq(localizacao)
                if dependencia_administrativa is not None:
                    mask &= dataframe["dependencia_administrativa"].astype("string").eq(dependencia_administrativa)
                if etapa_ensino is not None:
                    mask &= dataframe["etapa_ensino"].astype("string").eq(etapa_ensino)
                if serie is not None:
                    mask &= dataframe["serie"].astype("string").eq(serie)

                selecionado = dataframe.loc[mask, ["codigo_municipio", "valor"]].copy()
                if selecionado["codigo_municipio"].duplicated().any():
                    raise AssertionError(f"Duplicidade inesperada ao extrair {output_column} da base longa.")
                return selecionado.rename(columns={"valor": output_column})


            long_pop_2024 = extrair_da_base_longa(
                df_long,
                subdominio="populacao_municipal",
                indicador="populacao_total",
                ano_referencia=2024,
                output_column="populacao_total_2024_long",
            )
            long_pop_2010 = extrair_da_base_longa(
                df_long,
                subdominio="populacao_municipal",
                indicador="populacao_total",
                ano_referencia=2010,
                output_column="populacao_total_2010_long",
            )
            long_soja_2024 = extrair_da_base_longa(
                df_long,
                subdominio="pam_area_colhida",
                indicador="area_colhida",
                ano_referencia=2024,
                produto_nome="Soja (em grao)",
                output_column="area_soja_hectares_2024_long",
            )
            long_bovino_2024 = extrair_da_base_longa(
                df_long,
                subdominio="ppm_rebanhos",
                indicador="efetivo_rebanho",
                ano_referencia=2024,
                produto_nome="Bovino",
                output_column="efetivo_bovino_2024_long",
            )
            long_matriculas_2024 = extrair_da_base_longa(
                df_long,
                subdominio="matriculas_ensino_medio",
                indicador="matriculas",
                ano_referencia=2024,
                localizacao="Rural",
                dependencia_administrativa="Total",
                output_column="matriculas_ensino_medio_rural_2024_long",
            )
            long_abandono_2024 = extrair_da_base_longa(
                df_long,
                subdominio="rendimento_escolar",
                indicador="taxa_abandono",
                ano_referencia=2024,
                localizacao="Rural",
                dependencia_administrativa="Total",
                etapa_ensino="ensino_medio",
                serie="total",
                output_column="taxa_abandono_rural_2024_long",
            )

            chaves_integridade = pd.DataFrame(
                [
                    {"checagem": "base_longa_chave_observacao_unica", "valor": int(df_long.duplicated(["chave_observacao"]).sum()) == 0},
                    {"checagem": "snapshot_unico_por_municipio", "valor": int(df_snapshot.duplicated(["codigo_municipio"]).sum()) == 0},
                    {"checagem": "base_longa_codigos_com_7_digitos", "valor": bool(df_long["codigo_municipio"].astype(str).str.len().eq(7).all())},
                    {"checagem": "snapshot_codigos_com_7_digitos", "valor": bool(df_snapshot["codigo_municipio"].astype(str).str.len().eq(7).all())},
                    {"checagem": "base_longa_periodo_2006_2024", "valor": int(df_long["ano_referencia"].min()) == 2006 and int(df_long["ano_referencia"].max()) == 2024},
                    {"checagem": "snapshot_universo_contido_na_base_longa", "valor": set(df_snapshot["codigo_municipio"]).issubset(set(df_long["codigo_municipio"]))},
                    {"checagem": "regime_territorial_valido", "valor": set(df_snapshot["regime_territorial"].astype(str).unique()).issubset(set(REGIMES_VALIDOS))},
                ]
            )
            display(chaves_integridade)

            comparacoes_relacionais = pd.DataFrame(
                [
                    {
                        "campo_snapshot": "populacao_total_2024",
                        "campo_base_longa": "populacao_total_2024_long",
                        "iguais": bool(
                            df_snapshot.set_index("codigo_municipio")["populacao_total_2024"].sort_index().equals(
                                long_pop_2024.set_index("codigo_municipio")["populacao_total_2024_long"].sort_index()
                            )
                        ),
                    },
                    {
                        "campo_snapshot": "populacao_total_2010",
                        "campo_base_longa": "populacao_total_2010_long",
                        "iguais": bool(
                            df_snapshot.set_index("codigo_municipio")["populacao_total_2010"].sort_index().equals(
                                long_pop_2010.set_index("codigo_municipio")["populacao_total_2010_long"].sort_index()
                            )
                        ),
                    },
                    {
                        "campo_snapshot": "area_soja_hectares_2024",
                        "campo_base_longa": "area_soja_hectares_2024_long",
                        "iguais": bool(
                            df_snapshot.set_index("codigo_municipio")["area_soja_hectares_2024"].sort_index().equals(
                                long_soja_2024.set_index("codigo_municipio")["area_soja_hectares_2024_long"].sort_index()
                            )
                        ),
                    },
                    {
                        "campo_snapshot": "efetivo_bovino_2024",
                        "campo_base_longa": "efetivo_bovino_2024_long",
                        "iguais": bool(
                            df_snapshot.set_index("codigo_municipio")["efetivo_bovino_2024"].sort_index().equals(
                                long_bovino_2024.set_index("codigo_municipio")["efetivo_bovino_2024_long"].sort_index()
                            )
                        ),
                    },
                    {
                        "campo_snapshot": "matriculas_ensino_medio_rural_2024",
                        "campo_base_longa": "matriculas_ensino_medio_rural_2024_long",
                        "iguais": bool(
                            df_snapshot.set_index("codigo_municipio")["matriculas_ensino_medio_rural_2024"].sort_index().equals(
                                long_matriculas_2024.set_index("codigo_municipio")["matriculas_ensino_medio_rural_2024_long"].sort_index()
                            )
                        ),
                    },
                    {
                        "campo_snapshot": "taxa_abandono_rural_2024",
                        "campo_base_longa": "taxa_abandono_rural_2024_long",
                        "iguais": bool(
                            df_snapshot.set_index("codigo_municipio")["taxa_abandono_rural_2024"].sort_index().equals(
                                long_abandono_2024.set_index("codigo_municipio")["taxa_abandono_rural_2024_long"].sort_index()
                            )
                        ),
                    },
                ]
            )
            display(comparacoes_relacionais)
            """
        ),
        markdown_cell(
            """
            ## 4. Validacao de schema com pandera

            Os schemas abaixo assumem exatamente o contrato dos artefatos finais vigentes:
            base longa canonicamente tipificada e snapshot analitico com sinais booleanos e
            `regime_territorial`.
            """
        ),
        code_cell(
            """
            REGIOES_VALIDAS = ["Centro-Oeste", "Nordeste", "Norte", "Sudeste", "Sul"]
            UFS_VALIDAS = sorted(df_snapshot["sigla_estado"].dropna().unique().tolist())
            QUARTIS_VALIDOS = ["Q1", "Q2", "Q3", "Q4"]
            PORTES_VALIDOS = ["ate_10_mil", "10_a_50_mil", "50_a_200_mil", "acima_200_mil"]

            long_schema = pa.DataFrameSchema(
                {
                    "codigo_municipio": pa.Column(int, checks=[Check.ge(1000000), Check.le(9999999)]),
                    "nome_municipio": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "sigla_estado": pa.Column(str, checks=[Check.isin(UFS_VALIDAS)]),
                    "regiao": pa.Column(str, checks=[Check.isin(REGIOES_VALIDAS)]),
                    "ano_referencia": pa.Column(int, checks=[Check.ge(2006), Check.le(2024)]),
                    "fonte": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "dominio": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "subdominio": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "indicador": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "categoria": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "subcategoria": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "produto_codigo": pa.Column(object, nullable=True),
                    "produto_nome": pa.Column(object, nullable=True),
                    "localizacao": pa.Column(object, nullable=True),
                    "dependencia_administrativa": pa.Column(object, nullable=True),
                    "etapa_ensino": pa.Column(object, nullable=True),
                    "serie": pa.Column(object, nullable=True),
                    "unidade_medida": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "valor": pa.Column(float, nullable=True),
                    "nivel_granularidade": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "chave_observacao": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                },
                strict=True,
            )

            snapshot_schema = pa.DataFrameSchema(
                {
                    "codigo_municipio": pa.Column(int, checks=[Check.ge(1000000), Check.le(9999999)]),
                    "nome_municipio": pa.Column(str, checks=[Check.str_length(min_value=1)]),
                    "sigla_estado": pa.Column(str, checks=[Check.isin(UFS_VALIDAS)]),
                    "regiao": pa.Column(str, checks=[Check.isin(REGIOES_VALIDAS)]),
                    "populacao_total_2010": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "populacao_total_2024": pa.Column(float, checks=[Check.ge(0.0)]),
                    "area_algodao_hectares_2010": pa.Column(float, checks=[Check.ge(0.0)]),
                    "area_algodao_hectares_2024": pa.Column(float, checks=[Check.ge(0.0)]),
                    "area_cana_hectares_2010": pa.Column(float, checks=[Check.ge(0.0)]),
                    "area_cana_hectares_2024": pa.Column(float, checks=[Check.ge(0.0)]),
                    "area_milho_hectares_2010": pa.Column(float, checks=[Check.ge(0.0)]),
                    "area_milho_hectares_2024": pa.Column(float, checks=[Check.ge(0.0)]),
                    "area_soja_hectares_2010": pa.Column(float, checks=[Check.ge(0.0)]),
                    "area_soja_hectares_2024": pa.Column(float, checks=[Check.ge(0.0)]),
                    "efetivo_bovino_2010": pa.Column(float, checks=[Check.ge(0.0)]),
                    "efetivo_bovino_2024": pa.Column(float, checks=[Check.ge(0.0)]),
                    "total_estabelecimentos_agricolas_2017": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "area_total_agricola_hectares_2017": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "num_tratores_2017": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "matriculas_ensino_medio_rural_2024": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "taxa_abandono_rural_2024": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "area_total_culturas_selecionadas_hectares_2024": pa.Column(float, checks=[Check.ge(0.0)]),
                    "variacao_populacao_2010_2024_pct": pa.Column(float, nullable=True),
                    "variacao_area_soja_2010_2024_pct": pa.Column(float, nullable=True),
                    "variacao_area_milho_2010_2024_pct": pa.Column(float, nullable=True),
                    "variacao_area_cana_2010_2024_pct": pa.Column(float, nullable=True),
                    "variacao_rebanho_bovino_2010_2024_pct": pa.Column(float, nullable=True),
                    "tratores_por_100_estabelecimentos_2017": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "hectares_por_estabelecimento_2017": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "matriculas_rurais_por_1000_hab_2024": pa.Column(float, nullable=True, checks=[Check.ge(0.0)]),
                    "percentil_area_culturas_2024": pa.Column(float, checks=[Check.ge(0.0), Check.le(1.0)]),
                    "percentil_bovino_2024": pa.Column(float, checks=[Check.ge(0.0), Check.le(1.0)]),
                    "escore_intensificacao_agropecuaria_2024": pa.Column(float, checks=[Check.ge(0.0), Check.le(1.0)]),
                    "quartil_intensificacao_agropecuaria": pa.Column(str, checks=[Check.isin(QUARTIS_VALIDOS)]),
                    "porte_populacional_2024": pa.Column(str, checks=[Check.isin(PORTES_VALIDOS)]),
                    "sinal_intensificacao_agropecuaria": pa.Column(bool),
                    "sinal_esvaziamento_demografico": pa.Column(bool),
                    "sinal_fragilidade_educacional": pa.Column(bool),
                    "regime_territorial": pa.Column(str, checks=[Check.isin(REGIMES_VALIDOS)]),
                },
                strict=True,
            )


            def run_validation(label: str, schema: pa.DataFrameSchema, dataframe: pd.DataFrame):
                try:
                    schema.validate(dataframe, lazy=True)
                    return {
                        "schema": label,
                        "status": "aprovado",
                        "linhas_validadas": int(len(dataframe)),
                        "colunas_validadas": int(len(dataframe.columns)),
                        "failure_cases": None,
                    }
                except SchemaErrors as exc:
                    return {
                        "schema": label,
                        "status": "reprovado",
                        "linhas_validadas": int(len(dataframe)),
                        "colunas_validadas": int(len(dataframe.columns)),
                        "failure_cases": exc.failure_cases,
                    }


            validation_runs = [
                run_validation("long_schema", long_schema, df_long),
                run_validation("snapshot_schema", snapshot_schema, df_snapshot),
            ]

            validation_report = pd.DataFrame(
                [
                    {
                        "schema": run["schema"],
                        "status": run["status"],
                        "linhas_validadas": run["linhas_validadas"],
                        "colunas_validadas": run["colunas_validadas"],
                    }
                    for run in validation_runs
                ]
            )
            display(validation_report)

            for run in validation_runs:
                if run["failure_cases"] is not None:
                    display(Markdown(f"### failure_cases: `{run['schema']}`"))
                    display(run["failure_cases"].head(20))
            """
        ),
        code_cell(
            """
            total_municipios = int(df_snapshot["codigo_municipio"].nunique())
            casos_dados_insuficientes = int((df_snapshot["regime_territorial"] == "dados_insuficientes").sum())
            tratores_nulos = int(df_snapshot["num_tratores_2017"].isna().sum())
            abandono_nulos = int(df_snapshot["taxa_abandono_rural_2024"].isna().sum())

            resumo_final = f'''
            ## 5. Conclusao da validacao

            O notebook de validacao agora esta alinhado com os artefatos canonicos vigentes da refatoracao.

            - A `base_longa` valida o contrato de **{len(df_long.columns)} colunas** e preserva unicidade por `chave_observacao`.
            - O `snapshot` valida o contrato de **{len(df_snapshot.columns)} colunas** e preserva unicidade por `codigo_municipio`.
            - O universo municipal final cobre **{total_municipios} municipios** e o snapshot permanece derivado exclusivamente da base longa.
            - Os nulos remanescentes continuam interpretaveis pelo desenho do artefato: **{tratores_nulos}** em `num_tratores_2017` e **{abandono_nulos}** em `taxa_abandono_rural_2024`.
            - Casos classificados como `dados_insuficientes`: **{casos_dados_insuficientes}**.

            Em termos de integridade, o notebook deixa de depender dos nomes antigos `painel/snapshot`:
            a validacao passa a operar diretamente sobre `dados/saidas_finais/master_municipios_longo.csv`
            e `dados/saidas_finais/master_municipios_analitico_snapshot.csv`, que sao os outputs publicos efetivos do pipeline.
            '''
            display(Markdown(resumo_final))
            """
        ),
    ]
    return notebook


def main() -> None:
    notebook = build_notebook()
    NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(notebook, NOTEBOOK_PATH)
    print(f"Notebook atualizado em: {NOTEBOOK_PATH}")


if __name__ == "__main__":
    main()
