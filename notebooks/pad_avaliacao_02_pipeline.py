from __future__ import annotations

import io
import json
import os
import warnings
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen, urlretrieve

os.environ.setdefault("MPLCONFIGDIR", str(Path("/tmp") / "matplotlib"))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 180)


BASE_YEAR = 2006
SNAPSHOT_YEAR = 2024
CENSO_REFERENCE_YEAR = 2017
ANALYTIC_BASE_YEAR = 2010
LONG_SCHEMA = [
    "codigo_municipio",
    "nome_municipio",
    "sigla_estado",
    "regiao",
    "ano_referencia",
    "fonte",
    "dominio",
    "subdominio",
    "indicador",
    "categoria",
    "subcategoria",
    "produto_codigo",
    "produto_nome",
    "localizacao",
    "dependencia_administrativa",
    "etapa_ensino",
    "serie",
    "unidade_medida",
    "valor",
    "nivel_granularidade",
    "chave_observacao",
]
LONG_CATEGORICAL_COLUMNS = [
    "nome_municipio",
    "sigla_estado",
    "regiao",
    "fonte",
    "dominio",
    "subdominio",
    "indicador",
    "categoria",
    "subcategoria",
    "produto_codigo",
    "produto_nome",
    "localizacao",
    "dependencia_administrativa",
    "etapa_ensino",
    "serie",
    "unidade_medida",
    "nivel_granularidade",
]
PAM_PRODUCT_NAME_TO_CODE = {
    "Algodao herbaceo (em caroco)": "2689",
    "Cana-de-acucar": "2696",
    "Milho (em grao)": "2711",
    "Soja (em grao)": "2713",
}
PAM_PRODUCT_NAME_TO_ANALYTIC = {
    "Algodao herbaceo (em caroco)": "area_algodao_hectares",
    "Cana-de-acucar": "area_cana_hectares",
    "Milho (em grao)": "area_milho_hectares",
    "Soja (em grao)": "area_soja_hectares",
}
UF_TO_REGION = {
    "AC": "Norte",
    "AL": "Nordeste",
    "AP": "Norte",
    "AM": "Norte",
    "BA": "Nordeste",
    "CE": "Nordeste",
    "DF": "Centro-Oeste",
    "ES": "Sudeste",
    "GO": "Centro-Oeste",
    "MA": "Nordeste",
    "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    "MG": "Sudeste",
    "PA": "Norte",
    "PB": "Nordeste",
    "PR": "Sul",
    "PE": "Nordeste",
    "PI": "Nordeste",
    "RJ": "Sudeste",
    "RN": "Nordeste",
    "RS": "Sul",
    "RO": "Norte",
    "RR": "Norte",
    "SC": "Sul",
    "SP": "Sudeste",
    "SE": "Nordeste",
    "TO": "Norte",
}
STATE_NAME_TO_UF = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapa": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceara": "CE",
    "Distrito Federal": "DF",
    "Espirito Santo": "ES",
    "Goias": "GO",
    "Maranhao": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Para": "PA",
    "Paraiba": "PB",
    "Parana": "PR",
    "Pernambuco": "PE",
    "Piaui": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondonia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "Sao Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
}
RENDIMENTO_INDICATOR_MAP = {
    "Taxa de Aprovacao": "taxa_aprovacao",
    "Taxa de Reprovacao": "taxa_reprovacao",
    "Taxa de Abandono": "taxa_abandono",
}
RENDIMENTO_STAGE_MAP = {
    "Ensino Fundamental de 8 e 9 anos": "ensino_fundamental",
    "Ensino Medio": "ensino_medio",
}
REGIME_VALUES = [
    "intensificacao_com_esvaziamento_e_fragilidade",
    "intensificacao_com_fragilidade_educacional",
    "intensificacao_com_adaptacao_relativa",
    "baixa_pressao_territorial",
    "dados_insuficientes",
]


@dataclass
class PipelineArtifacts:
    configuracao: pd.DataFrame
    checkpoints_etl: pd.DataFrame
    base_longa: pd.DataFrame
    snapshot_analitico: pd.DataFrame
    validacoes: pd.DataFrame
    revisao_amostral: pd.DataFrame
    resumo_exportacao: pd.DataFrame
    tabela_descritiva: pd.DataFrame
    comparacao_quartis: pd.DataFrame
    comparacao_regiao: pd.DataFrame
    comparacao_porte: pd.DataFrame
    resumo_regimes: pd.DataFrame
    matriz_correlacao: pd.DataFrame
    municipios_destaque_regime: pd.DataFrame
    conclusao_markdown: str
    long_output_path: Path
    analytic_output_path: Path


def descobrir_raiz_projeto() -> Path:
    current_file = globals().get("__file__")
    candidatos = [
        Path.cwd().resolve(),
        Path.cwd().resolve().parent,
    ]
    if current_file:
        candidatos.append(Path(current_file).resolve().parent.parent)
    for candidato in candidatos:
        if (candidato / "dados").exists() and (candidato / "notebooks").exists():
            return candidato
    raise FileNotFoundError("Nao foi possivel localizar a raiz do projeto.")


PROJECT_ROOT = descobrir_raiz_projeto()
DATA_DIR = PROJECT_ROOT / "dados"
CACHE_DIR = DATA_DIR / "cache" / "protocolo_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_INPUTS = {
    "populacao": DATA_DIR / "br_ibge_populacao_municipio.csv",
    "sinopse_educacao": DATA_DIR / "Sinopse_Estatistica_da_Educacao_Basica_2024.xlsx",
    "sinopse_educacao_acento": DATA_DIR / "Sinopse_Estatistica_da_Educação_Basica_2024.xlsx",
    "rendimento_zip": DATA_DIR / "tx_rend_municipios_2024.zip",
    "pam_intermediario": DATA_DIR / "intermediarios" / "tabela1612_cleaned.csv",
}
LONG_OUTPUT = DATA_DIR / "saidas_finais" / "master_municipios_longo.csv"
ANALYTIC_OUTPUT = DATA_DIR / "saidas_finais" / "master_municipios_analitico_snapshot.csv"


def available_path(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Nenhum dos caminhos existe: {candidates}")


def safe_strip(value: object) -> object:
    if pd.isna(value):
        return pd.NA
    return str(value).strip()


def sanitize_label(value: object) -> str:
    if pd.isna(value):
        return ""
    texto = (
        str(value)
        .replace("á", "a")
        .replace("à", "a")
        .replace("ã", "a")
        .replace("â", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("õ", "o")
        .replace("ú", "u")
        .replace("ç", "c")
        .replace("Á", "A")
        .replace("À", "A")
        .replace("Ã", "A")
        .replace("Â", "A")
        .replace("É", "E")
        .replace("Ê", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ô", "O")
        .replace("Õ", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )
    texto = " ".join(texto.split())
    return texto.strip()


def normalizar_codigo_ibge(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .str.replace(".0", "", regex=False)
        .str.extract(r"(\d+)", expand=False)
    )


def filtrar_municipios_validos(dataframe: pd.DataFrame, coluna_codigo: str = "codigo_municipio") -> pd.DataFrame:
    resultado = dataframe.copy()
    resultado[coluna_codigo] = normalizar_codigo_ibge(resultado[coluna_codigo])
    resultado = resultado[resultado[coluna_codigo].str.len() == 7].copy()
    resultado[coluna_codigo] = resultado[coluna_codigo].astype(np.int32)
    return resultado


def validar_unicidade(dataframe: pd.DataFrame, chaves: list[str], nome: str) -> None:
    duplicados = int(dataframe.duplicated(subset=chaves).sum())
    if duplicados:
        raise AssertionError(f"{nome} possui {duplicados} duplicidades nas chaves {chaves}.")


def percentual_variacao(atual: pd.Series, base: pd.Series) -> pd.Series:
    atual = pd.to_numeric(atual, errors="coerce")
    base = pd.to_numeric(base, errors="coerce")
    return ((atual - base) / base.replace(0, np.nan)) * 100


def amplitude(series: pd.Series) -> float:
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty:
        return float("nan")
    return float(series.max() - series.min())


def resumir_descritivo(dataframe: pd.DataFrame, colunas: list[str]) -> pd.DataFrame:
    registros = []
    for coluna in colunas:
        serie = pd.to_numeric(dataframe[coluna], errors="coerce")
        registros.append(
            {
                "indicador": coluna,
                "frequencia": int(serie.notna().sum()),
                "media": serie.mean(),
                "mediana": serie.median(),
                "minimo": serie.min(),
                "maximo": serie.max(),
                "amplitude": amplitude(serie),
                "q1": serie.quantile(0.25),
                "q3": serie.quantile(0.75),
            }
        )
    return pd.DataFrame(registros)


def serie_percentil(series: pd.Series) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce").fillna(0)
    return series.rank(method="average", pct=True)


def categorizar_porte_populacional(series: pd.Series) -> pd.Series:
    bins = [-np.inf, 10000, 50000, 200000, np.inf]
    labels = ["ate_10_mil", "10_a_50_mil", "50_a_200_mil", "acima_200_mil"]
    return pd.cut(series, bins=bins, labels=labels)


def plotar_barras(series: pd.Series, titulo: str, ylabel: str) -> None:
    series.plot(kind="bar", color="#2c7fb8", figsize=(8, 4), title=titulo)
    plt.ylabel(ylabel)
    plt.xlabel("")
    plt.tight_layout()
    plt.show()


def garantir_arquivo(url: str, caminho_destino: Path) -> Path:
    caminho_destino.parent.mkdir(parents=True, exist_ok=True)
    if caminho_destino.exists():
        return caminho_destino
    urlretrieve(url, caminho_destino)
    return caminho_destino


def carregar_sidra_json_com_cache(nome_cache: str, urls: list[str]) -> pd.DataFrame:
    caminho_cache = CACHE_DIR / f"{nome_cache}.csv"
    if caminho_cache.exists():
        return pd.read_csv(caminho_cache)

    ultimo_erro: Exception | None = None
    for url in urls:
        try:
            with urlopen(url, timeout=20) as resposta:
                payload = json.load(resposta)
            dataframe = pd.DataFrame(payload[1:])
            dataframe.to_csv(caminho_cache, index=False, encoding="utf-8")
            return dataframe
        except Exception as exc:  # pragma: no cover - notebook runtime guard
            ultimo_erro = exc

    if ultimo_erro is None:
        raise RuntimeError(f"Nao foi possivel carregar {nome_cache}.")
    raise RuntimeError(f"Falha ao acessar SIDRA para {nome_cache}: {ultimo_erro}") from ultimo_erro


def urls_pam_expandidas() -> list[str]:
    years_list = ",".join(str(year) for year in range(BASE_YEAR, SNAPSHOT_YEAR + 1))
    return [
        (
            "https://apisidra.ibge.gov.br/values/t/1612/n6/all/v/216/"
            f"p/{years_list}/c81/all?formato=json"
        ),
        (
            "https://apisidra.ibge.gov.br/values/t/1612/n6/all/v/216/"
            f"p/{BASE_YEAR}-{SNAPSHOT_YEAR}/c81/all?formato=json"
        ),
    ]


def urls_ppm_expandidas() -> list[str]:
    years_list = ",".join(str(year) for year in range(BASE_YEAR, SNAPSHOT_YEAR + 1))
    return [
        (
            "https://apisidra.ibge.gov.br/values/t/3939/n6/all/v/105/"
            f"p/{years_list}/c79/all?formato=json"
        ),
        (
            "https://apisidra.ibge.gov.br/values/t/3939/n6/all/v/105/"
            f"p/{BASE_YEAR}-{SNAPSHOT_YEAR}/c79/all?formato=json"
        ),
    ]


def urls_censo_estrutura_expandidas() -> list[str]:
    return [
        (
            "https://apisidra.ibge.gov.br/values/t/6773/n6/all/v/183,184/p/2006,2017/"
            "c829/46302/c12559/41148/c12553/46523/c834/46527/c835/46528/c12598/41141?formato=json"
        ),
        (
            "https://apisidra.ibge.gov.br/values/t/6773/n6/all/v/183,184/p/2017/"
            "c829/46302/c12559/41148/c12553/46523/c834/46527/c835/46528/c12598/41141?formato=json"
        ),
    ]


def urls_censo_tratores_expandidas() -> list[str]:
    return [
        (
            "https://apisidra.ibge.gov.br/values/t/6873/n6/all/v/9572/p/2006,2017/"
            "c829/46302/c796/40597/c218/46502/c12517/113601?formato=json"
        ),
        (
            "https://apisidra.ibge.gov.br/values/t/6873/n6/all/v/9572/p/2017/"
            "c829/46302/c796/40597/c218/46502/c12517/113601?formato=json"
        ),
    ]


def carregar_lookup_municipios_inep(caminho_xlsx: Path) -> pd.DataFrame:
    dataframe = pd.read_excel(
        caminho_xlsx,
        sheet_name="1.2",
        skiprows=9,
        header=None,
        usecols=[0, 1, 2, 3],
    )
    dataframe.columns = ["regiao", "uf_extenso", "nome_municipio", "codigo_municipio"]
    dataframe = filtrar_municipios_validos(dataframe)
    dataframe["regiao"] = dataframe["regiao"].map(safe_strip)
    dataframe["uf_extenso"] = dataframe["uf_extenso"].map(safe_strip)
    dataframe["nome_municipio"] = dataframe["nome_municipio"].map(safe_strip)
    dataframe["uf_normalizado"] = dataframe["uf_extenso"].map(sanitize_label)
    dataframe["sigla_estado"] = dataframe["uf_normalizado"].map(STATE_NAME_TO_UF)
    dataframe["regiao"] = dataframe["sigla_estado"].map(UF_TO_REGION).fillna(dataframe["regiao"])
    dataframe = dataframe[["codigo_municipio", "nome_municipio", "sigla_estado", "regiao"]].drop_duplicates()
    validar_unicidade(dataframe, ["codigo_municipio"], "df_lookup_municipios")
    return dataframe.sort_values("codigo_municipio").reset_index(drop=True)


def carregar_populacao_longa(caminho_csv: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(caminho_csv)
    dataframe = dataframe.rename(
        columns={
            "ano": "ano_referencia",
            "id_municipio": "codigo_municipio",
            "populacao": "valor",
        }
    )
    dataframe = filtrar_municipios_validos(dataframe)
    dataframe["ano_referencia"] = pd.to_numeric(dataframe["ano_referencia"], errors="coerce").astype(np.int16)
    dataframe = dataframe[dataframe["ano_referencia"].between(BASE_YEAR, SNAPSHOT_YEAR)].copy()
    dataframe["valor"] = pd.to_numeric(dataframe["valor"], errors="coerce")
    dataframe["fonte"] = "IBGE"
    dataframe["dominio"] = "demografia"
    dataframe["subdominio"] = "populacao_municipal"
    dataframe["indicador"] = "populacao_total"
    dataframe["categoria"] = "variavel_demografica"
    dataframe["subcategoria"] = "populacao_residente_estimada"
    dataframe["produto_codigo"] = pd.NA
    dataframe["produto_nome"] = pd.NA
    dataframe["localizacao"] = pd.NA
    dataframe["dependencia_administrativa"] = pd.NA
    dataframe["etapa_ensino"] = pd.NA
    dataframe["serie"] = pd.NA
    dataframe["unidade_medida"] = "habitantes"
    dataframe["nivel_granularidade"] = "municipio_x_ano_x_variavel_demografica"
    dataframe = dataframe[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]
    validar_unicidade(dataframe, ["codigo_municipio", "ano_referencia", "indicador"], "df_populacao_longa")
    return dataframe.reset_index(drop=True)


def parse_pam_cache_dataframe(bruta: pd.DataFrame) -> pd.DataFrame:
    dataframe = bruta.rename(
        columns={
            "D1C": "codigo_municipio",
            "D3C": "ano_referencia",
            "D4C": "produto_codigo",
            "D4N": "produto_nome",
            "V": "valor",
            "MN": "unidade_medida",
        }
    )
    dataframe = filtrar_municipios_validos(dataframe)
    dataframe["ano_referencia"] = pd.to_numeric(dataframe["ano_referencia"], errors="coerce").astype("Int16")
    dataframe["produto_nome"] = dataframe["produto_nome"].map(sanitize_label)
    dataframe["produto_codigo"] = dataframe["produto_codigo"].astype("string")
    dataframe["valor"] = pd.to_numeric(
        dataframe["valor"].astype("string").str.replace("-", "", regex=False),
        errors="coerce",
    )
    dataframe["fonte"] = "IBGE"
    dataframe["dominio"] = "agropecuaria"
    dataframe["subdominio"] = "pam_area_colhida"
    dataframe["indicador"] = "area_colhida"
    dataframe["categoria"] = "lavoura_temporaria"
    dataframe["subcategoria"] = "produto_agricola"
    dataframe["localizacao"] = pd.NA
    dataframe["dependencia_administrativa"] = pd.NA
    dataframe["etapa_ensino"] = pd.NA
    dataframe["serie"] = pd.NA
    dataframe["nivel_granularidade"] = "municipio_x_ano_x_produto_x_metrica"
    return dataframe[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]


def parse_pam_intermediario_fallback(caminho_csv: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(caminho_csv)
    dataframe = dataframe.rename(
        columns={
            "ano_referencia": "ano_referencia",
            "codigo_municipio": "codigo_municipio",
            "produto_agricola": "produto_nome",
            "area_colhida_em_hectares": "valor",
        }
    )
    dataframe = filtrar_municipios_validos(dataframe)
    dataframe["ano_referencia"] = pd.to_numeric(dataframe["ano_referencia"], errors="coerce").astype("Int16")
    dataframe = dataframe[dataframe["ano_referencia"].between(BASE_YEAR, 2016)].copy()
    dataframe["produto_nome"] = dataframe["produto_nome"].map(sanitize_label)
    dataframe["produto_codigo"] = dataframe["produto_nome"].map(PAM_PRODUCT_NAME_TO_CODE).astype("string")
    dataframe["valor"] = pd.to_numeric(dataframe["valor"], errors="coerce")
    dataframe["fonte"] = "IBGE"
    dataframe["dominio"] = "agropecuaria"
    dataframe["subdominio"] = "pam_area_colhida"
    dataframe["indicador"] = "area_colhida"
    dataframe["categoria"] = "lavoura_temporaria"
    dataframe["subcategoria"] = "produto_agricola"
    dataframe["localizacao"] = pd.NA
    dataframe["dependencia_administrativa"] = pd.NA
    dataframe["etapa_ensino"] = pd.NA
    dataframe["serie"] = pd.NA
    dataframe["unidade_medida"] = "Hectares"
    dataframe["nivel_granularidade"] = "municipio_x_ano_x_produto_x_metrica"
    return dataframe[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]


def carregar_pam_longo() -> pd.DataFrame:
    componentes = []
    try:
        oficial = carregar_sidra_json_com_cache("pam_area_colhida_2006_2024_expandida", urls_pam_expandidas())
        componentes.append(parse_pam_cache_dataframe(oficial))
    except Exception:
        if LOCAL_INPUTS["pam_intermediario"].exists():
            componentes.append(parse_pam_intermediario_fallback(LOCAL_INPUTS["pam_intermediario"]))
        cache_atual = CACHE_DIR / "pam_area_colhida_2010_2024.csv"
        if cache_atual.exists():
            componentes.append(parse_pam_cache_dataframe(pd.read_csv(cache_atual)))

    if not componentes:
        raise FileNotFoundError("Nao foi possivel construir a camada longa da PAM.")

    dataframe = pd.concat(componentes, ignore_index=True)
    dataframe["prioridade_fonte"] = np.where(dataframe["ano_referencia"].eq(SNAPSHOT_YEAR), 2, 1)
    dataframe = dataframe.sort_values(
        ["codigo_municipio", "ano_referencia", "produto_nome", "prioridade_fonte"],
        kind="stable",
    )
    dataframe = dataframe.drop_duplicates(
        subset=["codigo_municipio", "ano_referencia", "produto_nome", "indicador"],
        keep="last",
    ).drop(columns="prioridade_fonte")
    validar_unicidade(
        dataframe,
        ["codigo_municipio", "ano_referencia", "produto_nome", "indicador"],
        "df_pam_longo",
    )
    return dataframe.reset_index(drop=True)


def parse_ppm_cache_dataframe(bruta: pd.DataFrame) -> pd.DataFrame:
    dataframe = bruta.rename(
        columns={
            "D1C": "codigo_municipio",
            "D3C": "ano_referencia",
            "D4C": "produto_codigo",
            "D4N": "produto_nome",
            "V": "valor",
            "MN": "unidade_medida",
        }
    )
    dataframe = filtrar_municipios_validos(dataframe)
    dataframe["ano_referencia"] = pd.to_numeric(dataframe["ano_referencia"], errors="coerce").astype("Int16")
    dataframe["produto_nome"] = dataframe["produto_nome"].map(sanitize_label)
    dataframe["produto_codigo"] = dataframe["produto_codigo"].astype("string")
    dataframe["valor"] = pd.to_numeric(
        dataframe["valor"].astype("string").str.replace("-", "", regex=False),
        errors="coerce",
    )
    dataframe["fonte"] = "IBGE"
    dataframe["dominio"] = "agropecuaria"
    dataframe["subdominio"] = "ppm_rebanhos"
    dataframe["indicador"] = "efetivo_rebanho"
    dataframe["categoria"] = "pecuaria"
    dataframe["subcategoria"] = "rebanho"
    dataframe["localizacao"] = pd.NA
    dataframe["dependencia_administrativa"] = pd.NA
    dataframe["etapa_ensino"] = pd.NA
    dataframe["serie"] = pd.NA
    dataframe["nivel_granularidade"] = "municipio_x_ano_x_rebanho_x_metrica"
    return dataframe[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]


def carregar_ppm_longo() -> pd.DataFrame:
    try:
        dataframe = parse_ppm_cache_dataframe(
            carregar_sidra_json_com_cache("ppm_rebanhos_2006_2024_expandida", urls_ppm_expandidas())
        )
    except Exception:
        cache_atual = CACHE_DIR / "ppm_bovinos_2010_2024.csv"
        dataframe = parse_ppm_cache_dataframe(pd.read_csv(cache_atual))

    validar_unicidade(
        dataframe,
        ["codigo_municipio", "ano_referencia", "produto_nome", "indicador"],
        "df_ppm_longo",
    )
    return dataframe.reset_index(drop=True)


def parse_censo_estrutura_dataframe(bruta: pd.DataFrame) -> pd.DataFrame:
    dataframe = bruta.rename(
        columns={
            "D1C": "codigo_municipio",
            "D2C": "variavel_codigo",
            "D2N": "variavel_nome",
            "D3C": "ano_referencia",
            "V": "valor",
            "MN": "unidade_medida",
        }
    )
    dataframe = filtrar_municipios_validos(dataframe)
    dataframe["ano_referencia"] = pd.to_numeric(dataframe["ano_referencia"], errors="coerce").astype("Int16")
    dataframe["variavel_codigo"] = dataframe["variavel_codigo"].astype("string")
    dataframe["variavel_nome"] = dataframe["variavel_nome"].map(sanitize_label)
    indicador_map = {
        "183": "numero_estabelecimentos_agropecuarios",
        "184": "area_estabelecimentos_agropecuarios",
    }
    dataframe["indicador"] = dataframe["variavel_codigo"].map(indicador_map)
    dataframe["valor"] = pd.to_numeric(
        dataframe["valor"].astype("string").str.replace("-", "", regex=False),
        errors="coerce",
    )
    dataframe["fonte"] = "IBGE"
    dataframe["dominio"] = "agropecuaria"
    dataframe["subdominio"] = "censo_agro_estrutura"
    dataframe["categoria"] = "estrutura_agropecuaria"
    dataframe["subcategoria"] = dataframe["variavel_nome"]
    dataframe["produto_codigo"] = dataframe["variavel_codigo"]
    dataframe["produto_nome"] = dataframe["variavel_nome"]
    dataframe["localizacao"] = pd.NA
    dataframe["dependencia_administrativa"] = pd.NA
    dataframe["etapa_ensino"] = pd.NA
    dataframe["serie"] = pd.NA
    dataframe["nivel_granularidade"] = "municipio_x_ano_x_variavel_censo_agro"
    dataframe = dataframe.dropna(subset=["indicador"])
    return dataframe[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]


def parse_censo_tratores_dataframe(bruta: pd.DataFrame) -> pd.DataFrame:
    dataframe = bruta.rename(
        columns={
            "D1C": "codigo_municipio",
            "D3C": "ano_referencia",
            "V": "valor",
            "MN": "unidade_medida",
        }
    )
    dataframe = filtrar_municipios_validos(dataframe)
    dataframe["ano_referencia"] = pd.to_numeric(dataframe["ano_referencia"], errors="coerce").astype("Int16")
    dataframe["valor"] = pd.to_numeric(
        dataframe["valor"].astype("string").str.replace("-", "", regex=False),
        errors="coerce",
    )
    dataframe["fonte"] = "IBGE"
    dataframe["dominio"] = "agropecuaria"
    dataframe["subdominio"] = "censo_agro_mecanizacao"
    dataframe["indicador"] = "numero_tratores"
    dataframe["categoria"] = "mecanizacao"
    dataframe["subcategoria"] = "tratores"
    dataframe["produto_codigo"] = "9572"
    dataframe["produto_nome"] = "Tratores"
    dataframe["localizacao"] = pd.NA
    dataframe["dependencia_administrativa"] = pd.NA
    dataframe["etapa_ensino"] = pd.NA
    dataframe["serie"] = pd.NA
    dataframe["nivel_granularidade"] = "municipio_x_ano_x_variavel_censo_agro"
    return dataframe[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]


def carregar_censo_agro_longo() -> pd.DataFrame:
    componentes = []
    try:
        componentes.append(
            parse_censo_estrutura_dataframe(
                carregar_sidra_json_com_cache(
                    "censo_agro_estrutura_2006_2017_expandida",
                    urls_censo_estrutura_expandidas(),
                )
            )
        )
    except Exception:
        componentes.append(parse_censo_estrutura_dataframe(pd.read_csv(CACHE_DIR / "censo_agro_estrutura_2017.csv")))

    try:
        componentes.append(
            parse_censo_tratores_dataframe(
                carregar_sidra_json_com_cache(
                    "censo_agro_tratores_2006_2017_expandida",
                    urls_censo_tratores_expandidas(),
                )
            )
        )
    except Exception:
        componentes.append(parse_censo_tratores_dataframe(pd.read_csv(CACHE_DIR / "censo_agro_tratores_2017.csv")))

    dataframe = pd.concat(componentes, ignore_index=True)
    validar_unicidade(
        dataframe,
        ["codigo_municipio", "ano_referencia", "subdominio", "indicador"],
        "df_censo_agro_longo",
    )
    return dataframe.reset_index(drop=True)


def flatten_measure_column(coluna: tuple) -> str:
    partes = [sanitize_label(item) for item in coluna if isinstance(item, str) and sanitize_label(item)]
    return " | ".join(partes)


def carregar_matriculas_inep_longo(caminho_xlsx: Path) -> pd.DataFrame:
    dataframe = pd.read_excel(caminho_xlsx, sheet_name="1.26", header=[5, 6, 7, 8])
    flattened_columns = []
    for coluna in dataframe.columns:
        if isinstance(coluna, tuple):
            codigo_final = str(coluna[-1]).strip()
            if codigo_final == "Unnamed: 0_level_3":
                flattened_columns.append("regiao_inep")
            elif codigo_final == "Unnamed: 1_level_3":
                flattened_columns.append("uf_extenso")
            elif codigo_final == "Unnamed: 2_level_3":
                flattened_columns.append("nome_municipio_inep")
            elif codigo_final == "Unnamed: 3_level_3":
                flattened_columns.append("codigo_municipio")
            else:
                flattened_columns.append(flatten_measure_column(coluna))
        else:
            flattened_columns.append(str(coluna))
    dataframe.columns = flattened_columns
    measure_columns = [col for col in dataframe.columns if str(col).startswith("Numero de Matriculas do Ensino Medio")]
    rename_map = {}
    for coluna in measure_columns:
        if "Total1-3" in coluna:
            rename_map[coluna] = "total_geral"
        elif "Urbana" in coluna and coluna.endswith("Total"):
            rename_map[coluna] = "urbana_total"
        elif "Urbana" in coluna and coluna.endswith("Federal"):
            rename_map[coluna] = "urbana_federal"
        elif "Urbana" in coluna and coluna.endswith("Estadual"):
            rename_map[coluna] = "urbana_estadual"
        elif "Urbana" in coluna and coluna.endswith("Municipal"):
            rename_map[coluna] = "urbana_municipal"
        elif "Urbana" in coluna and coluna.endswith("Privada"):
            rename_map[coluna] = "urbana_privada"
        elif "Rural" in coluna and coluna.endswith("Total"):
            rename_map[coluna] = "rural_total"
        elif "Rural" in coluna and coluna.endswith("Federal"):
            rename_map[coluna] = "rural_federal"
        elif "Rural" in coluna and coluna.endswith("Estadual"):
            rename_map[coluna] = "rural_estadual"
        elif "Rural" in coluna and coluna.endswith("Municipal"):
            rename_map[coluna] = "rural_municipal"
        elif "Rural" in coluna and coluna.endswith("Privada"):
            rename_map[coluna] = "rural_privada"
    dataframe = dataframe.rename(columns=rename_map)
    id_columns = ["codigo_municipio", "regiao_inep", "uf_extenso", "nome_municipio_inep"]

    dataframe = filtrar_municipios_validos(dataframe)
    value_columns = [
        "total_geral",
        "urbana_total",
        "urbana_federal",
        "urbana_estadual",
        "urbana_municipal",
        "urbana_privada",
        "rural_total",
        "rural_federal",
        "rural_estadual",
        "rural_municipal",
        "rural_privada",
    ]
    dataframe = dataframe[id_columns + value_columns].copy()
    melted = dataframe.melt(
        id_vars=id_columns,
        value_vars=value_columns,
        var_name="medida",
        value_name="valor",
    )
    melted["valor"] = pd.to_numeric(melted["valor"], errors="coerce")
    descriptor_map = {
        "total_geral": ("Total", "Total"),
        "urbana_total": ("Urbana", "Total"),
        "urbana_federal": ("Urbana", "Federal"),
        "urbana_estadual": ("Urbana", "Estadual"),
        "urbana_municipal": ("Urbana", "Municipal"),
        "urbana_privada": ("Urbana", "Privada"),
        "rural_total": ("Rural", "Total"),
        "rural_federal": ("Rural", "Federal"),
        "rural_estadual": ("Rural", "Estadual"),
        "rural_municipal": ("Rural", "Municipal"),
        "rural_privada": ("Rural", "Privada"),
    }
    melted["localizacao"] = melted["medida"].map(lambda value: descriptor_map[value][0])
    melted["dependencia_administrativa"] = melted["medida"].map(lambda value: descriptor_map[value][1])
    melted["ano_referencia"] = np.int16(SNAPSHOT_YEAR)
    melted["fonte"] = "INEP"
    melted["dominio"] = "educacao"
    melted["subdominio"] = "matriculas_ensino_medio"
    melted["indicador"] = "matriculas"
    melted["categoria"] = "matricula_escolar"
    melted["subcategoria"] = "ensino_medio_regular"
    melted["produto_codigo"] = pd.NA
    melted["produto_nome"] = pd.NA
    melted["etapa_ensino"] = "ensino_medio"
    melted["serie"] = "total"
    melted["unidade_medida"] = "matriculas"
    melted["nivel_granularidade"] = "municipio_x_ano_x_localizacao_x_dependencia_x_indicador"
    melted = melted[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]
    validar_unicidade(
        melted,
        ["codigo_municipio", "ano_referencia", "localizacao", "dependencia_administrativa", "indicador"],
        "df_matriculas_longo",
    )
    return melted.reset_index(drop=True)


def carregar_matriculas_ensino_medio_cor_raca_longo(caminho_xlsx: Path) -> pd.DataFrame:
    dataframe = pd.read_excel(caminho_xlsx, sheet_name="1.27", header=[5, 6, 7, 8])
    original_columns = list(dataframe.columns)
    flattened_columns = []
    measure_metadata = {}
    for coluna in original_columns:
        if isinstance(coluna, tuple):
            codigo_final = str(coluna[-1]).strip()
            if codigo_final == "Unnamed: 0_level_3":
                flattened_columns.append("regiao_inep")
            elif codigo_final == "Unnamed: 1_level_3":
                flattened_columns.append("uf_extenso")
            elif codigo_final == "Unnamed: 2_level_3":
                flattened_columns.append("nome_municipio_inep")
            elif codigo_final == "Unnamed: 3_level_3":
                flattened_columns.append("codigo_municipio")
            else:
                nome_coluna = flatten_measure_column(coluna)
                flattened_columns.append(nome_coluna)
                partes = [
                    sanitize_label(item)
                    for item in coluna[1:]
                    if isinstance(item, str) and "Unnamed" not in item and sanitize_label(item)
                ]
                if "Total1-3" in nome_coluna:
                    measure_metadata[nome_coluna] = "total_geral"
                else:
                    measure_metadata[nome_coluna] = "_".join(parte.lower().replace(" ", "_") for parte in partes)
        else:
            flattened_columns.append(str(coluna))
    dataframe.columns = flattened_columns
    dataframe = filtrar_municipios_validos(dataframe)
    measure_columns = [col for col in dataframe.columns if col in measure_metadata]
    melted = dataframe.melt(
        id_vars=["codigo_municipio", "regiao_inep", "uf_extenso", "nome_municipio_inep"],
        value_vars=measure_columns,
        var_name="medida",
        value_name="valor",
    )
    melted["valor"] = pd.to_numeric(melted["valor"], errors="coerce")
    melted["ano_referencia"] = np.int16(SNAPSHOT_YEAR)
    melted["fonte"] = "INEP"
    melted["dominio"] = "educacao"
    melted["subdominio"] = "matriculas_ensino_medio_sexo_cor_raca"
    melted["indicador"] = "matriculas"
    melted["categoria"] = "sexo_cor_raca"
    melted["subcategoria"] = melted["medida"].map(measure_metadata)
    melted["produto_codigo"] = pd.NA
    melted["produto_nome"] = pd.NA
    melted["localizacao"] = pd.NA
    melted["dependencia_administrativa"] = pd.NA
    melted["etapa_ensino"] = "ensino_medio"
    melted["serie"] = "total"
    melted["unidade_medida"] = "matriculas"
    melted["nivel_granularidade"] = "municipio_x_ano_x_categoria_x_subcategoria"
    melted = melted[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]
    validar_unicidade(
        melted,
        ["codigo_municipio", "ano_referencia", "subdominio", "subcategoria"],
        "df_matriculas_cor_raca_longo",
    )
    return melted.reset_index(drop=True)


def carregar_matriculas_ensino_medio_tempo_jornada_longo(caminho_xlsx: Path) -> pd.DataFrame:
    dataframe = pd.read_excel(caminho_xlsx, sheet_name="1.29", header=[5, 6, 7, 8])
    original_columns = list(dataframe.columns)
    flattened_columns = []
    measure_metadata = {}
    for coluna in original_columns:
        if isinstance(coluna, tuple):
            codigo_final = str(coluna[-1]).strip()
            if codigo_final == "Unnamed: 0_level_3":
                flattened_columns.append("regiao_inep")
            elif codigo_final == "Unnamed: 1_level_3":
                flattened_columns.append("uf_extenso")
            elif codigo_final == "Unnamed: 2_level_3":
                flattened_columns.append("nome_municipio_inep")
            elif codigo_final == "Unnamed: 3_level_3":
                flattened_columns.append("codigo_municipio")
            else:
                nome_coluna = flatten_measure_column(coluna)
                flattened_columns.append(nome_coluna)
                partes = [
                    sanitize_label(item)
                    for item in coluna[1:]
                    if isinstance(item, str) and "Unnamed" not in item and sanitize_label(item)
                ]
                if "Total1-3" in nome_coluna:
                    measure_metadata[nome_coluna] = {"subcategoria": "total_geral", "dependencia": "Total"}
                else:
                    jornada = partes[1].lower().replace(" ", "_").replace("4", "").replace("5", "")
                    dependencia = partes[2] if len(partes) > 2 else "Total"
                    measure_metadata[nome_coluna] = {
                        "subcategoria": jornada,
                        "dependencia": dependencia,
                    }
        else:
            flattened_columns.append(str(coluna))
    dataframe.columns = flattened_columns
    dataframe = filtrar_municipios_validos(dataframe)
    measure_columns = [col for col in dataframe.columns if col in measure_metadata]
    melted = dataframe.melt(
        id_vars=["codigo_municipio", "regiao_inep", "uf_extenso", "nome_municipio_inep"],
        value_vars=measure_columns,
        var_name="medida",
        value_name="valor",
    )
    metadata_df = pd.DataFrame.from_dict(measure_metadata, orient="index").rename_axis("medida").reset_index()
    melted = melted.merge(metadata_df, on="medida", how="left")
    melted["valor"] = pd.to_numeric(melted["valor"], errors="coerce")
    melted["ano_referencia"] = np.int16(SNAPSHOT_YEAR)
    melted["fonte"] = "INEP"
    melted["dominio"] = "educacao"
    melted["subdominio"] = "matriculas_ensino_medio_tempo_jornada"
    melted["indicador"] = "matriculas"
    melted["categoria"] = "tempo_jornada"
    melted["produto_codigo"] = pd.NA
    melted["produto_nome"] = pd.NA
    melted["localizacao"] = pd.NA
    melted["dependencia_administrativa"] = melted["dependencia"]
    melted["etapa_ensino"] = "ensino_medio"
    melted["serie"] = "total"
    melted["unidade_medida"] = "matriculas"
    melted["nivel_granularidade"] = "municipio_x_ano_x_categoria_x_dependencia"
    melted = melted.rename(columns={"subcategoria": "subcategoria"})
    melted = melted[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]
    validar_unicidade(
        melted,
        ["codigo_municipio", "ano_referencia", "subdominio", "subcategoria", "dependencia_administrativa"],
        "df_matriculas_tempo_jornada_longo",
    )
    return melted.reset_index(drop=True)


def carregar_rendimento_inep_longo(caminho_zip: Path) -> pd.DataFrame:
    with zipfile.ZipFile(caminho_zip) as arquivo_zip:
        nome_xlsx = next(nome for nome in arquivo_zip.namelist() if nome.endswith(".xlsx"))
        payload = io.BytesIO(arquivo_zip.read(nome_xlsx))

    dataframe = pd.read_excel(payload, sheet_name="MUNICIPIOS ", header=[5, 6, 7, 8])
    original_columns = list(dataframe.columns)
    measure_metadata = {}
    for coluna in original_columns:
        if isinstance(coluna, tuple):
            codigo_final = str(coluna[-1]).strip()
            if codigo_final.startswith(("1_", "2_", "3_")):
                indicador = sanitize_label(coluna[0])
                etapa = sanitize_label(coluna[1])
                serie = sanitize_label(coluna[2]).replace(" ", "_").replace("-", "_")
                measure_metadata[codigo_final] = {
                    "indicador": RENDIMENTO_INDICATOR_MAP[indicador],
                    "etapa_ensino": RENDIMENTO_STAGE_MAP[etapa],
                    "serie": serie.lower().replace("__", "_"),
                }
    flattened_columns = []
    for coluna in dataframe.columns:
        if isinstance(coluna, tuple):
            codigo_final = str(coluna[-1]).strip()
            flattened_columns.append(codigo_final)
        else:
            flattened_columns.append(str(coluna))
    dataframe.columns = flattened_columns
    dataframe = dataframe.rename(
        columns={
            "NU_ANO_CENSO": "ano_referencia",
            "NO_REGIAO": "regiao_inep",
            "SG_UF": "sigla_estado_inep",
            "CO_MUNICIPIO": "codigo_municipio",
            "NO_MUNICIPIO": "nome_municipio_inep",
            "NO_CATEGORIA": "localizacao",
            "NO_DEPENDENCIA": "dependencia_administrativa",
        }
    )
    measure_columns = [col for col in dataframe.columns if str(col).startswith(("1_", "2_", "3_"))]
    dataframe = filtrar_municipios_validos(dataframe)
    dataframe["ano_referencia"] = pd.to_numeric(dataframe["ano_referencia"], errors="coerce").astype("Int16")
    dataframe["localizacao"] = dataframe["localizacao"].map(safe_strip)
    dataframe["dependencia_administrativa"] = dataframe["dependencia_administrativa"].map(safe_strip)
    melted = dataframe.melt(
        id_vars=["codigo_municipio", "ano_referencia", "localizacao", "dependencia_administrativa"],
        value_vars=measure_columns,
        var_name="medida",
        value_name="valor",
    )
    metadata_df = pd.DataFrame.from_dict(measure_metadata, orient="index").rename_axis("medida").reset_index()
    melted = melted.merge(metadata_df, on="medida", how="left")
    melted["valor"] = pd.to_numeric(
        melted["valor"].astype("string").str.replace("--", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    melted["fonte"] = "INEP"
    melted["dominio"] = "educacao"
    melted["subdominio"] = "rendimento_escolar"
    melted["categoria"] = "taxa_rendimento"
    melted["subcategoria"] = "ensino_basico"
    melted["produto_codigo"] = pd.NA
    melted["produto_nome"] = pd.NA
    melted["unidade_medida"] = "percentual"
    melted["nivel_granularidade"] = (
        "municipio_x_ano_x_localizacao_x_dependencia_x_indicador_x_etapa_x_serie"
    )
    melted = melted[
        [
            "codigo_municipio",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
        ]
    ]
    validar_unicidade(
        melted,
        [
            "codigo_municipio",
            "ano_referencia",
            "localizacao",
            "dependencia_administrativa",
            "indicador",
            "etapa_ensino",
            "serie",
        ],
        "df_rendimento_longo",
    )
    return melted.reset_index(drop=True)


def enriquecer_territorio(dataframe: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    enriched = dataframe.merge(lookup, on="codigo_municipio", how="left")
    missing_lookup = int(enriched["nome_municipio"].isna().sum())
    if missing_lookup:
        raise AssertionError(f"Ha {missing_lookup} observacoes sem correspondencia municipal no lookup.")
    return enriched


def gerar_chave_observacao(dataframe: pd.DataFrame) -> pd.Series:
    key_columns = [
        "codigo_municipio",
        "ano_referencia",
        "fonte",
        "subdominio",
        "indicador",
        "produto_codigo",
        "produto_nome",
        "localizacao",
        "dependencia_administrativa",
        "etapa_ensino",
        "serie",
        "categoria",
        "subcategoria",
    ]
    hashed = pd.util.hash_pandas_object(
        dataframe[key_columns].astype("string").fillna("<NA>"),
        index=False,
    )
    return hashed.astype("uint64")


def otimizar_dataframe_longo(dataframe: pd.DataFrame) -> pd.DataFrame:
    resultado = dataframe.copy()
    resultado["codigo_municipio"] = resultado["codigo_municipio"].astype(np.int32)
    resultado["ano_referencia"] = pd.to_numeric(resultado["ano_referencia"], errors="coerce").astype(np.int16)
    resultado["valor"] = pd.to_numeric(resultado["valor"], errors="coerce")
    resultado["chave_observacao"] = resultado["chave_observacao"].astype("uint64")
    for coluna in LONG_CATEGORICAL_COLUMNS:
        resultado[coluna] = resultado[coluna].astype("category")
    return resultado


def construir_base_longa() -> tuple[pd.DataFrame, pd.DataFrame]:
    sinopse_path = available_path(
        LOCAL_INPUTS["sinopse_educacao"],
        LOCAL_INPUTS["sinopse_educacao_acento"],
    )
    lookup = carregar_lookup_municipios_inep(sinopse_path)

    componentes = {
        "populacao": carregar_populacao_longa(LOCAL_INPUTS["populacao"]),
        "pam": carregar_pam_longo(),
        "ppm": carregar_ppm_longo(),
        "censo_agro": carregar_censo_agro_longo(),
        "matriculas": carregar_matriculas_inep_longo(sinopse_path),
        "matriculas_cor_raca": carregar_matriculas_ensino_medio_cor_raca_longo(sinopse_path),
        "matriculas_tempo_jornada": carregar_matriculas_ensino_medio_tempo_jornada_longo(sinopse_path),
        "rendimento": carregar_rendimento_inep_longo(LOCAL_INPUTS["rendimento_zip"]),
    }
    checkpoints = pd.DataFrame(
        [
            {
                "dataset": nome,
                "linhas": len(dataframe),
                "colunas": len(dataframe.columns),
                "anos_min": pd.to_numeric(dataframe["ano_referencia"], errors="coerce").min(),
                "anos_max": pd.to_numeric(dataframe["ano_referencia"], errors="coerce").max(),
            }
            for nome, dataframe in componentes.items()
        ]
    )

    base_longa = pd.concat(componentes.values(), ignore_index=True, sort=False)
    base_longa = enriquecer_territorio(base_longa, lookup)
    base_longa["regiao"] = base_longa["sigla_estado"].map(UF_TO_REGION).fillna(base_longa["regiao"])
    base_longa["chave_observacao"] = gerar_chave_observacao(base_longa)
    base_longa = base_longa[
        [
            "codigo_municipio",
            "nome_municipio",
            "sigla_estado",
            "regiao",
            "ano_referencia",
            "fonte",
            "dominio",
            "subdominio",
            "indicador",
            "categoria",
            "subcategoria",
            "produto_codigo",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "etapa_ensino",
            "serie",
            "unidade_medida",
            "valor",
            "nivel_granularidade",
            "chave_observacao",
        ]
    ]
    validar_unicidade(base_longa, ["chave_observacao"], "df_base_longa")
    base_longa = otimizar_dataframe_longo(base_longa)
    return base_longa, checkpoints


def extrair_medida(
    base_longa: pd.DataFrame,
    *,
    subdominio: str,
    indicador: str,
    output_column: str,
    ano_referencia: int | None = None,
    produto_nome: str | None = None,
    localizacao: str | None = None,
    dependencia_administrativa: str | None = None,
    etapa_ensino: str | None = None,
    serie: str | None = None,
) -> pd.DataFrame:
    dataframe = base_longa
    mask = dataframe["subdominio"].astype("string").eq(subdominio)
    mask &= dataframe["indicador"].astype("string").eq(indicador)
    if ano_referencia is not None:
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
    validar_unicidade(selecionado, ["codigo_municipio"], f"extracao_{output_column}")
    return selecionado.rename(columns={"valor": output_column})


def construir_snapshot_analitico(base_longa: pd.DataFrame) -> pd.DataFrame:
    lookup = (
        base_longa[["codigo_municipio", "nome_municipio", "sigla_estado", "regiao"]]
        .drop_duplicates(subset=["codigo_municipio"])
        .sort_values("codigo_municipio")
        .reset_index(drop=True)
    )

    snapshot = lookup.copy()
    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="populacao_municipal",
            indicador="populacao_total",
            ano_referencia=ANALYTIC_BASE_YEAR,
            output_column="populacao_total_2010",
        ),
        on="codigo_municipio",
        how="left",
    )
    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="populacao_municipal",
            indicador="populacao_total",
            ano_referencia=SNAPSHOT_YEAR,
            output_column="populacao_total_2024",
        ),
        on="codigo_municipio",
        how="left",
    )

    for produto_nome, output_column in PAM_PRODUCT_NAME_TO_ANALYTIC.items():
        snapshot = snapshot.merge(
            extrair_medida(
                base_longa,
                subdominio="pam_area_colhida",
                indicador="area_colhida",
                ano_referencia=ANALYTIC_BASE_YEAR,
                produto_nome=produto_nome,
                output_column=f"{output_column}_2010",
            ),
            on="codigo_municipio",
            how="left",
        )
        snapshot = snapshot.merge(
            extrair_medida(
                base_longa,
                subdominio="pam_area_colhida",
                indicador="area_colhida",
                ano_referencia=SNAPSHOT_YEAR,
                produto_nome=produto_nome,
                output_column=f"{output_column}_2024",
            ),
            on="codigo_municipio",
            how="left",
        )

    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="ppm_rebanhos",
            indicador="efetivo_rebanho",
            ano_referencia=ANALYTIC_BASE_YEAR,
            produto_nome="Bovino",
            output_column="efetivo_bovino_2010",
        ),
        on="codigo_municipio",
        how="left",
    )
    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="ppm_rebanhos",
            indicador="efetivo_rebanho",
            ano_referencia=SNAPSHOT_YEAR,
            produto_nome="Bovino",
            output_column="efetivo_bovino_2024",
        ),
        on="codigo_municipio",
        how="left",
    )
    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="censo_agro_estrutura",
            indicador="numero_estabelecimentos_agropecuarios",
            ano_referencia=CENSO_REFERENCE_YEAR,
            output_column="total_estabelecimentos_agricolas_2017",
        ),
        on="codigo_municipio",
        how="left",
    )
    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="censo_agro_estrutura",
            indicador="area_estabelecimentos_agropecuarios",
            ano_referencia=CENSO_REFERENCE_YEAR,
            output_column="area_total_agricola_hectares_2017",
        ),
        on="codigo_municipio",
        how="left",
    )
    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="censo_agro_mecanizacao",
            indicador="numero_tratores",
            ano_referencia=CENSO_REFERENCE_YEAR,
            output_column="num_tratores_2017",
        ),
        on="codigo_municipio",
        how="left",
    )
    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="matriculas_ensino_medio",
            indicador="matriculas",
            ano_referencia=SNAPSHOT_YEAR,
            localizacao="Rural",
            dependencia_administrativa="Total",
            output_column="matriculas_ensino_medio_rural_2024",
        ),
        on="codigo_municipio",
        how="left",
    )
    snapshot = snapshot.merge(
        extrair_medida(
            base_longa,
            subdominio="rendimento_escolar",
            indicador="taxa_abandono",
            ano_referencia=SNAPSHOT_YEAR,
            localizacao="Rural",
            dependencia_administrativa="Total",
            etapa_ensino="ensino_medio",
            serie="total",
            output_column="taxa_abandono_rural_2024",
        ),
        on="codigo_municipio",
        how="left",
    )

    colunas_zero = [
        "area_algodao_hectares_2010",
        "area_algodao_hectares_2024",
        "area_cana_hectares_2010",
        "area_cana_hectares_2024",
        "area_milho_hectares_2010",
        "area_milho_hectares_2024",
        "area_soja_hectares_2010",
        "area_soja_hectares_2024",
        "efetivo_bovino_2010",
        "efetivo_bovino_2024",
    ]
    for coluna in colunas_zero:
        snapshot[coluna] = pd.to_numeric(snapshot[coluna], errors="coerce").fillna(0)

    snapshot["area_total_culturas_selecionadas_hectares_2024"] = snapshot[
        [
            "area_algodao_hectares_2024",
            "area_cana_hectares_2024",
            "area_milho_hectares_2024",
            "area_soja_hectares_2024",
        ]
    ].sum(axis=1)
    snapshot["variacao_populacao_2010_2024_pct"] = percentual_variacao(
        snapshot["populacao_total_2024"],
        snapshot["populacao_total_2010"],
    )
    snapshot["variacao_area_soja_2010_2024_pct"] = percentual_variacao(
        snapshot["area_soja_hectares_2024"],
        snapshot["area_soja_hectares_2010"],
    )
    snapshot["variacao_area_milho_2010_2024_pct"] = percentual_variacao(
        snapshot["area_milho_hectares_2024"],
        snapshot["area_milho_hectares_2010"],
    )
    snapshot["variacao_area_cana_2010_2024_pct"] = percentual_variacao(
        snapshot["area_cana_hectares_2024"],
        snapshot["area_cana_hectares_2010"],
    )
    snapshot["variacao_rebanho_bovino_2010_2024_pct"] = percentual_variacao(
        snapshot["efetivo_bovino_2024"],
        snapshot["efetivo_bovino_2010"],
    )
    snapshot["tratores_por_100_estabelecimentos_2017"] = (
        snapshot["num_tratores_2017"] / snapshot["total_estabelecimentos_agricolas_2017"].replace(0, np.nan)
    ) * 100
    snapshot["hectares_por_estabelecimento_2017"] = (
        snapshot["area_total_agricola_hectares_2017"]
        / snapshot["total_estabelecimentos_agricolas_2017"].replace(0, np.nan)
    )
    snapshot["matriculas_rurais_por_1000_hab_2024"] = (
        snapshot["matriculas_ensino_medio_rural_2024"]
        / snapshot["populacao_total_2024"].replace(0, np.nan)
    ) * 1000
    snapshot["percentil_area_culturas_2024"] = serie_percentil(
        snapshot["area_total_culturas_selecionadas_hectares_2024"]
    )
    snapshot["percentil_bovino_2024"] = serie_percentil(snapshot["efetivo_bovino_2024"])
    snapshot["escore_intensificacao_agropecuaria_2024"] = (
        snapshot["percentil_area_culturas_2024"] + snapshot["percentil_bovino_2024"]
    ) / 2
    snapshot["quartil_intensificacao_agropecuaria"] = pd.qcut(
        snapshot["escore_intensificacao_agropecuaria_2024"].rank(method="first"),
        4,
        labels=["Q1", "Q2", "Q3", "Q4"],
    )
    snapshot["porte_populacional_2024"] = categorizar_porte_populacional(snapshot["populacao_total_2024"])

    q1_matriculas = snapshot["matriculas_rurais_por_1000_hab_2024"].quantile(0.25)
    q3_abandono = snapshot["taxa_abandono_rural_2024"].quantile(0.75)
    critical_columns = [
        "variacao_populacao_2010_2024_pct",
        "matriculas_rurais_por_1000_hab_2024",
        "taxa_abandono_rural_2024",
        "escore_intensificacao_agropecuaria_2024",
    ]
    dados_insuficientes = snapshot[critical_columns].isna().any(axis=1)
    snapshot["sinal_intensificacao_agropecuaria"] = (
        snapshot["escore_intensificacao_agropecuaria_2024"] >= 0.75
    ).fillna(False).astype(bool)
    snapshot["sinal_esvaziamento_demografico"] = (
        snapshot["variacao_populacao_2010_2024_pct"] < 0
    ).fillna(False).astype(bool)
    snapshot["sinal_fragilidade_educacional"] = (
        (snapshot["taxa_abandono_rural_2024"] >= q3_abandono)
        | (snapshot["matriculas_rurais_por_1000_hab_2024"] <= q1_matriculas)
    ).fillna(False).astype(bool)
    snapshot["regime_territorial"] = "baixa_pressao_territorial"

    mask_intensificacao = snapshot["sinal_intensificacao_agropecuaria"]
    mask_esvaziamento = snapshot["sinal_esvaziamento_demografico"]
    mask_fragilidade = snapshot["sinal_fragilidade_educacional"]

    snapshot.loc[
        mask_intensificacao & ~mask_fragilidade & ~mask_esvaziamento,
        "regime_territorial",
    ] = "intensificacao_com_adaptacao_relativa"
    snapshot.loc[
        mask_intensificacao & mask_fragilidade,
        "regime_territorial",
    ] = "intensificacao_com_fragilidade_educacional"
    snapshot.loc[
        mask_intensificacao & mask_esvaziamento & mask_fragilidade,
        "regime_territorial",
    ] = "intensificacao_com_esvaziamento_e_fragilidade"
    snapshot.loc[dados_insuficientes, "regime_territorial"] = "dados_insuficientes"

    snapshot = (
        snapshot.assign(
            _regime_ordem=pd.Categorical(
                snapshot["regime_territorial"],
                categories=REGIME_VALUES,
                ordered=True,
            ).codes
        )
        .sort_values(
            ["_regime_ordem", "escore_intensificacao_agropecuaria_2024", "taxa_abandono_rural_2024"],
            ascending=[True, False, False],
        )
        .drop(columns="_regime_ordem")
        .reset_index(drop=True)
    )
    validar_unicidade(snapshot, ["codigo_municipio"], "df_snapshot_analitico")
    return snapshot


def validar_outputs(base_longa: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    expected_domains = {
        "demografia",
        "agropecuaria",
        "educacao",
    }
    expected_subdominios = {
        "populacao_municipal",
        "pam_area_colhida",
        "ppm_rebanhos",
        "censo_agro_estrutura",
        "censo_agro_mecanizacao",
        "matriculas_ensino_medio",
        "rendimento_escolar",
    }
    validations = pd.DataFrame(
        [
            {
                "checagem": "base_longa_codigos_com_7_digitos",
                "valor": bool(base_longa["codigo_municipio"].astype(str).str.len().eq(7).all()),
            },
            {
                "checagem": "snapshot_codigos_com_7_digitos",
                "valor": bool(snapshot["codigo_municipio"].astype(str).str.len().eq(7).all()),
            },
            {
                "checagem": "chave_observacao_unica",
                "valor": int(base_longa.duplicated(["chave_observacao"]).sum()) == 0,
            },
            {
                "checagem": "valor_numerico",
                "valor": bool(pd.api.types.is_numeric_dtype(base_longa["valor"])),
            },
            {
                "checagem": "unidade_medida_preenchida",
                "valor": bool(base_longa["unidade_medida"].astype("string").notna().all()),
            },
            {
                "checagem": "fonte_preenchida",
                "valor": bool(base_longa["fonte"].astype("string").notna().all()),
            },
            {
                "checagem": "dominio_preenchido",
                "valor": bool(base_longa["dominio"].astype("string").notna().all()),
            },
            {
                "checagem": "base_longa_mais_de_4_milhoes",
                "valor": len(base_longa) > 4_000_000,
            },
            {
                "checagem": "cobertura_dominios_minima",
                "valor": expected_domains.issubset(set(base_longa["dominio"].astype(str).unique())),
            },
            {
                "checagem": "cobertura_subdominios_minima",
                "valor": expected_subdominios.issubset(set(base_longa["subdominio"].astype(str).unique())),
            },
            {
                "checagem": "snapshot_derivado_da_base_longa",
                "valor": int(snapshot["codigo_municipio"].nunique()) == 5570,
            },
            {
                "checagem": "snapshot_regimes_validos",
                "valor": set(snapshot["regime_territorial"].astype(str).unique()).issubset(set(REGIME_VALUES)),
            },
            {
                "checagem": "snapshot_sinais_booleanos",
                "valor": all(
                    set(snapshot[coluna].dropna().unique()).issubset({True, False})
                    for coluna in [
                        "sinal_intensificacao_agropecuaria",
                        "sinal_esvaziamento_demografico",
                        "sinal_fragilidade_educacional",
                    ]
                ),
            },
        ]
    )
    return validations


def criar_revisao_amostral(base_longa: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    sample_long = base_longa.sample(10, random_state=42)[
        [
            "codigo_municipio",
            "nome_municipio",
            "ano_referencia",
            "fonte",
            "subdominio",
            "indicador",
            "produto_nome",
            "localizacao",
            "dependencia_administrativa",
            "valor",
        ]
    ].sort_values(["codigo_municipio", "ano_referencia"])
    sample_snapshot = snapshot.sample(5, random_state=42)[
        [
            "codigo_municipio",
            "nome_municipio",
            "sigla_estado",
            "populacao_total_2024",
            "area_soja_hectares_2024",
            "efetivo_bovino_2024",
            "matriculas_ensino_medio_rural_2024",
            "taxa_abandono_rural_2024",
            "regime_territorial",
        ]
    ].sort_values("codigo_municipio")
    sample_snapshot["fonte_amostra"] = "snapshot_analitico"
    sample_long["fonte_amostra"] = "base_longa"
    return pd.concat([sample_long, sample_snapshot], ignore_index=True, sort=False)


def exportar_artefatos(base_longa: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    LONG_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    base_longa.to_csv(LONG_OUTPUT, index=False, encoding="utf-8")
    snapshot.to_csv(ANALYTIC_OUTPUT, index=False, encoding="utf-8")
    return pd.DataFrame(
        [
            {"artefato": LONG_OUTPUT.name, "gerado": LONG_OUTPUT.exists(), "linhas": len(base_longa)},
            {"artefato": ANALYTIC_OUTPUT.name, "gerado": ANALYTIC_OUTPUT.exists(), "linhas": len(snapshot)},
        ]
    )


def construir_tabelas_analiticas(
    snapshot: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    descriptive_columns = [
        "variacao_populacao_2010_2024_pct",
        "variacao_area_soja_2010_2024_pct",
        "variacao_area_milho_2010_2024_pct",
        "variacao_area_cana_2010_2024_pct",
        "variacao_rebanho_bovino_2010_2024_pct",
        "tratores_por_100_estabelecimentos_2017",
        "hectares_por_estabelecimento_2017",
        "matriculas_rurais_por_1000_hab_2024",
        "taxa_abandono_rural_2024",
    ]
    tabela_descritiva = resumir_descritivo(snapshot, descriptive_columns)
    comparacao_quartis = (
        snapshot.groupby("quartil_intensificacao_agropecuaria", observed=True)[
            [
                "variacao_populacao_2010_2024_pct",
                "matriculas_rurais_por_1000_hab_2024",
                "taxa_abandono_rural_2024",
                "tratores_por_100_estabelecimentos_2017",
            ]
        ]
        .mean()
        .round(2)
        .reset_index()
    )
    comparacao_regiao = (
        snapshot.groupby("regiao", observed=True)[
            [
                "variacao_populacao_2010_2024_pct",
                "matriculas_rurais_por_1000_hab_2024",
                "taxa_abandono_rural_2024",
                "escore_intensificacao_agropecuaria_2024",
            ]
        ]
        .mean()
        .round(2)
        .reset_index()
    )
    comparacao_porte = (
        snapshot.groupby("porte_populacional_2024", observed=True)[
            [
                "variacao_populacao_2010_2024_pct",
                "matriculas_rurais_por_1000_hab_2024",
                "taxa_abandono_rural_2024",
            ]
        ]
        .mean()
        .round(2)
        .reset_index()
    )
    resumo_regimes = (
        snapshot.groupby("regime_territorial", observed=True)[
            [
                "variacao_populacao_2010_2024_pct",
                "matriculas_rurais_por_1000_hab_2024",
                "taxa_abandono_rural_2024",
                "escore_intensificacao_agropecuaria_2024",
            ]
        ]
        .mean()
        .reindex(REGIME_VALUES)
        .round(2)
        .reset_index()
    )
    contagem_regimes = snapshot["regime_territorial"].value_counts().reindex(REGIME_VALUES, fill_value=0)
    resumo_regimes.insert(1, "municipios", contagem_regimes.values)
    resumo_regimes.insert(2, "participacao_pct", ((contagem_regimes / len(snapshot)) * 100).round(2).values)
    correlation_columns = [
        "variacao_populacao_2010_2024_pct",
        "matriculas_rurais_por_1000_hab_2024",
        "taxa_abandono_rural_2024",
        "tratores_por_100_estabelecimentos_2017",
        "hectares_por_estabelecimento_2017",
        "escore_intensificacao_agropecuaria_2024",
    ]
    matriz_correlacao = snapshot[correlation_columns].corr(numeric_only=True).round(3)
    municipios_destaque_regime = (
        snapshot.assign(
            _regime_ordem=pd.Categorical(
                snapshot["regime_territorial"],
                categories=REGIME_VALUES,
                ordered=True,
            ).codes
        )
        .sort_values(
            ["_regime_ordem", "escore_intensificacao_agropecuaria_2024", "taxa_abandono_rural_2024"],
            ascending=[True, False, False],
        )
        .groupby("regime_territorial", observed=True, group_keys=False)
        .head(5)
        .copy()
    )
    municipios_destaque_regime["ordem_no_regime"] = (
        municipios_destaque_regime.groupby("regime_territorial", observed=True).cumcount() + 1
    )
    municipios_destaque_regime = municipios_destaque_regime[
        [
            "regime_territorial",
            "ordem_no_regime",
            "codigo_municipio",
            "nome_municipio",
            "sigla_estado",
            "regiao",
            "sinal_intensificacao_agropecuaria",
            "sinal_esvaziamento_demografico",
            "sinal_fragilidade_educacional",
            "variacao_populacao_2010_2024_pct",
            "taxa_abandono_rural_2024",
            "matriculas_rurais_por_1000_hab_2024",
            "escore_intensificacao_agropecuaria_2024",
        ]
    ].reset_index(drop=True)
    return (
        tabela_descritiva,
        comparacao_quartis,
        comparacao_regiao,
        comparacao_porte,
        resumo_regimes,
        matriz_correlacao,
        municipios_destaque_regime,
    )


def construir_conclusao(snapshot: pd.DataFrame, matriz_correlacao: pd.DataFrame, base_longa: pd.DataFrame) -> str:
    total_municipios = len(snapshot)
    contagem_regimes = snapshot["regime_territorial"].value_counts().reindex(REGIME_VALUES, fill_value=0)
    correlacao_abandono = matriz_correlacao.loc["taxa_abandono_rural_2024", "escore_intensificacao_agropecuaria_2024"]
    correlacao_pop = matriz_correlacao.loc["variacao_populacao_2010_2024_pct", "escore_intensificacao_agropecuaria_2024"]
    destaques = (
        snapshot[
            snapshot["regime_territorial"].isin(
                [
                    "intensificacao_com_esvaziamento_e_fragilidade",
                    "intensificacao_com_fragilidade_educacional",
                ]
            )
        ]
        .head(5)[["nome_municipio", "sigla_estado"]]
        .apply(lambda row: f"{row['nome_municipio']} ({row['sigla_estado']})", axis=1)
        .tolist()
    )
    return f"""
## 6. Conclusoes interpretativas

O artefato opera com base longa multigranular e interpreta o snapshot final por meio de regimes territoriais de transformacao, em chave descritiva e multicausal.

- Universo exportado na camada longa: **{len(base_longa):,} observacoes**.
- Universo do snapshot analitico: **{total_municipios} municipios**.
- Intensificacao com esvaziamento e fragilidade: **{int(contagem_regimes['intensificacao_com_esvaziamento_e_fragilidade'])}** municipios.
- Intensificacao com fragilidade educacional: **{int(contagem_regimes['intensificacao_com_fragilidade_educacional'])}** municipios.
- Intensificacao com adaptacao relativa: **{int(contagem_regimes['intensificacao_com_adaptacao_relativa'])}** municipios.
- Baixa pressao territorial: **{int(contagem_regimes['baixa_pressao_territorial'])}** municipios.
- Dados insuficientes: **{int(contagem_regimes['dados_insuficientes'])}** municipios.
- Associacao simples entre intensificacao agropecuaria e abandono rural: **{correlacao_abandono:.3f}**.
- Associacao simples entre intensificacao agropecuaria e variacao populacional: **{correlacao_pop:.3f}**.

Municipios destacados nos regimes de maior pressao territorial observada:

{chr(10).join(f"- {item}" for item in destaques) if destaques else "- Nenhum municipio classificado nos regimes de maior pressao."}

Limites metodologicos mantidos:

- sem aprendizado de maquina;
- sem previsao;
- sem inferencia causal;
- uso descritivo e comparativo;
- agregacao tardia derivada exclusivamente da base longa.
"""


def run_pipeline(export: bool = True) -> PipelineArtifacts:
    configuracao = pd.DataFrame(
        [
            {"parametro": "PROJECT_ROOT", "valor": str(PROJECT_ROOT)},
            {"parametro": "CACHE_DIR", "valor": str(CACHE_DIR)},
            {"parametro": "LONG_OUTPUT", "valor": str(LONG_OUTPUT)},
            {"parametro": "ANALYTIC_OUTPUT", "valor": str(ANALYTIC_OUTPUT)},
            {"parametro": "BASE_YEAR", "valor": BASE_YEAR},
            {"parametro": "SNAPSHOT_YEAR", "valor": SNAPSHOT_YEAR},
            {"parametro": "ANALYTIC_BASE_YEAR", "valor": ANALYTIC_BASE_YEAR},
        ]
    )
    base_longa, checkpoints_etl = construir_base_longa()
    snapshot = construir_snapshot_analitico(base_longa)
    validacoes = validar_outputs(base_longa, snapshot)
    revisao_amostral = criar_revisao_amostral(base_longa, snapshot)
    resumo_exportacao = exportar_artefatos(base_longa, snapshot) if export else pd.DataFrame()
    (
        tabela_descritiva,
        comparacao_quartis,
        comparacao_regiao,
        comparacao_porte,
        resumo_regimes,
        matriz_correlacao,
        municipios_destaque_regime,
    ) = construir_tabelas_analiticas(snapshot)
    conclusao_markdown = construir_conclusao(snapshot, matriz_correlacao, base_longa)
    return PipelineArtifacts(
        configuracao=configuracao,
        checkpoints_etl=checkpoints_etl,
        base_longa=base_longa,
        snapshot_analitico=snapshot,
        validacoes=validacoes,
        revisao_amostral=revisao_amostral,
        resumo_exportacao=resumo_exportacao,
        tabela_descritiva=tabela_descritiva,
        comparacao_quartis=comparacao_quartis,
        comparacao_regiao=comparacao_regiao,
        comparacao_porte=comparacao_porte,
        resumo_regimes=resumo_regimes,
        matriz_correlacao=matriz_correlacao,
        municipios_destaque_regime=municipios_destaque_regime,
        conclusao_markdown=conclusao_markdown,
        long_output_path=LONG_OUTPUT,
        analytic_output_path=ANALYTIC_OUTPUT,
    )
