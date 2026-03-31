from __future__ import annotations

from inspect import cleandoc
from pathlib import Path

import nbformat as nbf


PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "pad_avaliacao_02.ipynb"


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
            # PAD Avaliacao 02 - Regimes Territoriais de Transformacao

            Este notebook aplica o enquadramento metodologico do **PROJETO DE PESQUISA DOIS**
            ao fluxo principal do PAD. A estrategia continua baseada em uma camada longa,
            auditavel e multigranular, mas o snapshot final passa a ser interpretado por meio de
            **regimes territoriais de transformacao**, e nao mais por uma faixa unica de atencao.

            Os artefatos publicos canonicos sao:

            - `dados/saidas_finais/master_municipios_longo.csv`
            - `dados/saidas_finais/master_municipios_analitico_snapshot.csv`

            O objetivo analitico aqui e descritivo e comparativo: identificar combinacoes entre
            intensificacao agropecuaria, dinamica demografica agregada e fragilidade educacional
            rural, sem inferencia causal e sem modelagem preditiva.
            """
        ),
        code_cell(
            """
            from __future__ import annotations

            import sys
            from pathlib import Path

            import matplotlib.pyplot as plt
            import pandas as pd
            from IPython.display import Markdown, display

            cwd = Path.cwd().resolve()
            candidate_paths = [
                cwd / "notebooks",
                cwd,
                cwd.parent / "notebooks",
            ]
            for candidate in candidate_paths:
                if candidate.exists() and str(candidate) not in sys.path:
                    sys.path.append(str(candidate))

            from pad_avaliacao_02_pipeline import REGIME_VALUES, plotar_barras, run_pipeline

            plt.style.use("seaborn-v0_8-whitegrid")
            artifacts = run_pipeline(export=True)
            """
        ),
        markdown_cell(
            """
            ## 1. Configuracao e checkpoints por fonte

            Esta secao explicita o contrato da pipeline e resume a construcao da camada longa
            antes de qualquer agregacao municipal final.
            """
        ),
        code_cell(
            """
            display(artifacts.configuracao)
            display(artifacts.checkpoints_etl)
            """
        ),
        markdown_cell(
            """
            ## 2. Camada longa consolidada

            Cada linha representa uma observacao tipificada por `fonte`, `dominio`,
            `subdominio`, `indicador`, `unidade_medida` e `nivel_granularidade`.
            A chave `chave_observacao` precisa ser unica.
            """
        ),
        code_cell(
            """
            display(artifacts.base_longa.head(10))
            display(
                pd.DataFrame(
                    [
                        {"metrica": "linhas_base_longa", "valor": len(artifacts.base_longa)},
                        {"metrica": "municipios_unicos", "valor": artifacts.base_longa["codigo_municipio"].nunique()},
                        {"metrica": "subdominios_unicos", "valor": artifacts.base_longa["subdominio"].astype(str).nunique()},
                        {"metrica": "chaves_unicas", "valor": artifacts.base_longa["chave_observacao"].nunique()},
                    ]
                )
            )
            """
        ),
        markdown_cell(
            """
            ## 3. Snapshot analitico derivado da base longa

            O snapshot municipal e reconstruido exclusivamente a partir da camada longa.
            Os sinais booleanos e o `regime_territorial` tornam explicita a heuristica
            interpretativa adotada no projeto.
            """
        ),
        code_cell(
            """
            display(artifacts.snapshot_analitico.head(10))
            """
        ),
        markdown_cell(
            """
            ## 4. Validacoes, exportacao e revisao amostral

            Aqui verificamos integridade estrutural, cobertura minima dos dominios,
            validade dos regimes e amostras auditaveis dos dois artefatos finais.
            """
        ),
        code_cell(
            """
            display(artifacts.validacoes)
            display(artifacts.revisao_amostral.head(15))
            display(artifacts.resumo_exportacao)
            """
        ),
        markdown_cell(
            """
            ## 5. Estatistica descritiva, comparacoes e regimes territoriais

            A leitura substantiva continua baseada em estatistica descritiva e comparacao
            estruturada entre grupos, mas o produto sintese agora e uma tipologia de regimes
            territoriais de transformacao.
            """
        ),
        code_cell(
            """
            display(artifacts.tabela_descritiva)
            display(artifacts.comparacao_quartis)
            display(artifacts.comparacao_regiao)
            display(artifacts.comparacao_porte)
            display(artifacts.resumo_regimes)
            display(artifacts.matriz_correlacao)

            regime_series = (
                artifacts.resumo_regimes.set_index("regime_territorial")["municipios"]
                .reindex(REGIME_VALUES)
                .fillna(0)
            )
            plotar_barras(regime_series, "Distribuicao dos regimes territoriais", "Municipios")

            plt.figure(figsize=(8, 4))
            artifacts.snapshot_analitico["variacao_populacao_2010_2024_pct"].dropna().plot.hist(
                bins=30,
                color="#4daf4a",
                title="Distribuicao da variacao populacional 2010-2024",
            )
            plt.xlabel("Variacao populacional (%)")
            plt.tight_layout()
            plt.show()

            boxplot_df = artifacts.snapshot_analitico.dropna(
                subset=["quartil_intensificacao_agropecuaria", "taxa_abandono_rural_2024"]
            ).copy()
            plt.figure(figsize=(8, 4))
            boxplot_df.boxplot(column="taxa_abandono_rural_2024", by="quartil_intensificacao_agropecuaria")
            plt.title("Abandono rural por quartil de intensificacao agropecuaria")
            plt.suptitle("")
            plt.xlabel("Quartil de intensificacao")
            plt.ylabel("Taxa de abandono rural 2024")
            plt.tight_layout()
            plt.show()

            scatter_df = artifacts.snapshot_analitico.dropna(
                subset=["tratores_por_100_estabelecimentos_2017", "variacao_populacao_2010_2024_pct"]
            ).copy()
            plt.figure(figsize=(8, 5))
            plt.scatter(
                scatter_df["tratores_por_100_estabelecimentos_2017"],
                scatter_df["variacao_populacao_2010_2024_pct"],
                alpha=0.45,
                color="#e41a1c",
            )
            plt.title("Mecanizacao e variacao populacional")
            plt.xlabel("Tratores por 100 estabelecimentos (2017)")
            plt.ylabel("Variacao populacional 2010-2024 (%)")
            plt.tight_layout()
            plt.show()

            display(artifacts.municipios_destaque_regime)
            """
        ),
        code_cell(
            """
            display(Markdown(artifacts.conclusao_markdown))
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
