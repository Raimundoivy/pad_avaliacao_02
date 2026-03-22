# %% [markdown]
# # **Formatos de Armazenamento**

# %% [markdown]
# **Pesquisas e implementações sobre tipos de arquivos utilizados na análise de dados**

# %% [markdown]
# Origem/Histórico - Estrutura - Encoding - Compressão

# %%
import os
import time
import psutil
import pathlib
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as orc
import fastavro
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
import warnings

# %%
warnings.filterwarnings('ignore')
sns.set_theme(style="whitegrid")
os.makedirs('output_formats', exist_ok=True)

# %%
master_csv = 'dados/master_municipios.csv'

# %%
def get_size_mb(p):
    path = pathlib.Path(p)
    if path.is_dir():
        return sum(f.stat().st_size for f in path.rglob('*') if f.is_file()) / 1e6
    return path.stat().st_size / 1e6 if path.exists() else 0.0

results = []
workflow_start = time.time()

# %%
proc = psutil.Process()

print(f"Configuração concluída. Usando a fonte CSV: {master_csv}")

def _t(fn):
    s = time.time(); fn(); return time.time() - s

SEM_COMPRESSAO = 'Sem Compressão'

# %%
df_polars = pl.scan_csv(master_csv, ignore_errors=True).select([
    'codigo_municipio', 'nome_municipio', 'sigla_estado', 'populacao_total'
]).collect()

# %%
df_pandas = df_polars.to_pandas()

# %%
print("Iniciando o Spark...")
spark = (SparkSession.builder.appName("Benchmark")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .enableHiveSupport()  
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
df_spark = spark.createDataFrame(df_pandas)

# %%
avro_schema = {
    'type': 'record', 'name': 'root', 
    'fields': [
        {'name': 'codigo_municipio', 'type': ['null', 'long']},
        {'name': 'nome_municipio', 'type': ['null', 'string']},
        {'name': 'sigla_estado', 'type': ['null', 'string']},
        {'name': 'populacao_total', 'type': ['null', 'string']}
    ]
}
print(f"Workflow preparado com {len(df_pandas)} rows.")


# %% [markdown]
# **1 – Baseados em Texto**

# %% [markdown]
# **CSV (Comma-Separated Values)** — Formato texto tabelar simples, amplamente usado em planilhas e bancos de dados desde os anos 1970–1980. Sem esquema formal, sem compressão nativa, sem tipos de dados definidos.
# 
# | Aspecto | Detalhe |
# |---|---|
# | **Estrutura** | Linhas de texto separadas por delimitador (vírgula/ponto-e-vírgula); cabeçalho opcional na 1ª linha |
# | **Encoding** | Texto puro UTF-8; sem serialização binária |
# | **Compressão** | Nenhuma nativa; pode-se aplicar gzip/zip externamente |
# | **Ponto forte** | Máxima portabilidade e legibilidade humana; suporte universal |
# | **Ponto fraco** | Sem tipos, sem schema, sem compressão — ineficiente para Big Data |

# %% [markdown]
# **Referências:** [RFC 4180 — CSV Format](https://www.ietf.org/rfc/rfc4180.txt) | [Formatos Suportados — Azure Synapse](https://learn.microsoft.com/pt-pt/previous-versions/azure/synapse-analytics/data-explorer/ingest-data/data-explorer-ingest-data-supported-formats)

# %%
print('--- Benchmarking CSV ---')
file_path = f"output_formats/data_{SEM_COMPRESSAO}.csv"

mem_start = proc.memory_info().rss
w_time = _t(lambda: df_polars.write_csv(file_path))
w_mem = max(0.1, (proc.memory_info().rss - mem_start) / 1e6)

r_time = _t(lambda: pl.read_csv(file_path, ignore_errors=True))
m_time = _t(lambda: pl.read_csv(file_path, n_rows=1, ignore_errors=True))
c_time = _t(lambda: pl.read_csv(file_path, columns=['codigo_municipio', 'populacao_total'], ignore_errors=True))

results.append({"Format": "CSV", "Compression": SEM_COMPRESSAO,
    "Write Time (s)": w_time, "Read Time (s)": r_time,
    "Meta Time (s)": m_time, "Col Time (s)": c_time,
    "Size (MB)": get_size_mb(file_path), "Peak RAM (MB)": w_mem})
print(f"✔️ CSV OK — shape: {df_polars.shape}")


# %% [markdown]
# **XML (Extensible Markup Language)** — Formato de marcação derivado do SGML (ISO 8879), criado pelo W3C em 1996. Estrutura hierárquica com tags explícitas. Muito usado em configurações, SOAP e troca de dados legada.
# 
# | Aspecto | Detalhe |
# |---|---|
# | **Estrutura** | Árvore hierárquica de elementos com tags de abertura/fechamento e atributos |
# | **Encoding** | Texto UTF-8/UTF-16; verboso por natureza (tags repetidas por registro) |
# | **Compressão** | Minificação (remove espaços); gzip/Brotli externos; EXI (Efficient XML Interchange) binário W3C |
# | **Ponto forte** | Autodescritivo, validável via XSD/DTD, suporte a namespaces e dados hierárquicos complexos |
# | **Ponto fraco** | Extremamente verboso → arquivos grandes; parsing custoso comparado a JSON/binários |

# %% [markdown]
# **Referências:** [W3C XML](https://www.w3.org/XML/) | [W3C XML Activity](https://www.w3.org/XML/Activity) | [W3C XML Notes](https://www.w3.org/XML/notes.html) | [W3C XML Data Model](https://www.w3.org/XML/Datamodel.html) | [W3C XML Structure Theory](https://www.w3.org/XML/9711theory/xmlstruct.html)

# %%
print('--- Benchmarking XML ---')
file_path, mult = f"output_formats/data_{SEM_COMPRESSAO}.xml", len(df_pandas) / 500000
df_amostra = df_pandas.head(500000)

mem_start = proc.memory_info().rss
w_time = _t(lambda: df_amostra.to_xml(file_path, index=False, parser='etree', compression=None))
w_mem = max(0.1, (proc.memory_info().rss - mem_start) / 1e6)

r_time = _t(lambda: pd.read_xml(file_path))
m_time = _t(lambda: pd.read_xml(file_path, xpath="./*[1]"))
c_time = _t(lambda: pd.read_xml(file_path)[['codigo_municipio', 'populacao_total']])

results.append({"Format": "XML (Est.)", "Compression": SEM_COMPRESSAO,
    "Write Time (s)": w_time*mult, "Read Time (s)": r_time*mult,
    "Meta Time (s)": m_time*mult, "Col Time (s)": c_time*mult,
    "Size (MB)": get_size_mb(file_path)*mult, "Peak RAM (MB)": w_mem*mult})
print("✔️ XML OK")


# %% [markdown]
# **JSON (JavaScript Object Notation)** — Criado por Douglas Crockford (2001) para serializar objetos JavaScript. Tornou-se o padrão de troca de dados em APIs REST. Independente de linguagem (famílias C, Python, Java, etc.).
# 
# | Aspecto | Detalhe |
# |---|---|
# | **Estrutura** | Pares chave/valor (objetos `{}`) e listas ordenadas (arrays `[]`); aninhamento livre |
# | **Encoding** | Texto UTF-8; sem tipos binários nativos (números, strings, bool, null) |
# | **Compressão** | Minificação (`separators=(',',':')`); gzip/Brotli externos; formatos binários alternativos: MessagePack, CBOR |
# | **Ponto forte** | Legibilidade humana, suporte nativo em browsers/APIs, parsing simples |
# | **Ponto fraco** | Verboso (chaves repetidas por registro); sem schema formal; ineficiente para analytics em escala |

# %% [markdown]
# **Referências:** [JSON.org](https://www.json.org/json-en.html) | [MDN — Content-Encoding](https://developer.mozilla.org/pt-BR/docs/Web/HTTP/Reference/Headers/Content-Encoding) | [Comprimindo JSON para Mobile](https://moldstud.com/articles/p-step-by-step-guide-to-compressing-json-for-enhanced-mobile-application-performance)

# %%
print('--- Benchmarking JSON ---')

for name, comp in [(SEM_COMPRESSAO, None), ('gzip', 'gzip')]:
    file_path = f"output_formats/data_{name}.json"

    mem_start = proc.memory_info().rss
    w_time = _t(lambda: df_pandas.to_json(file_path, orient='records', lines=True, compression=comp))
    w_mem = max(0.1, (proc.memory_info().rss - mem_start) / 1e6)

    r_time = _t(lambda: pl.read_ndjson(file_path, infer_schema_length=10000))
    m_time = _t(lambda: pl.scan_ndjson(file_path, infer_schema_length=10000).head(0).collect())
    c_time = _t(lambda: pl.scan_ndjson(file_path, infer_schema_length=10000).select(['codigo_municipio', 'populacao_total']).collect())

    results.append({"Format": "JSON", "Compression": name,
        "Write Time (s)": w_time, "Read Time (s)": r_time,
        "Meta Time (s)": m_time, "Col Time (s)": c_time,
        "Size (MB)": get_size_mb(file_path), "Peak RAM (MB)": w_mem})
    print(f"✔️ JSON {name} OK")


# %% [markdown]
# **Apache Parquet** — Formato colunar colaborativo (Twitter + Cloudera), fundamentado no paper Dremel da Google. Padrão de fato no ecossistema Spark/Data Lakehouse (Snowflake, Delta Lake, Apache Iceberg).
# 
# | Aspecto | Detalhe |
# |---|---|
# | **Estrutura** | Row Groups → Column Chunks → Pages; footer serializado via Apache Thrift TCompactProtocol |
# | **Encoding** | Dictionary, RLE/Bit-Packing híbrido, DELTA_BINARY_PACKED para timestamps/sequências |
# | **Compressão** | Redução de até 70% antes do compressor externo; suporta GZIP, Snappy, ZSTD |
# | **Ponto forte** | Definition/Repetition Levels habilitam dado aninhado sem overhead; máxima compatibilidade |
# | **Ponto fraco** | Subótimo para leituras de linhas completas em padrões OLTP |
# 
# **Referências:** [Parquet File Format](https://parquet.apache.org/docs/file-format/) | [Parquet Concepts](https://parquet.apache.org/docs/concepts/) | [GitHub parquet-format](https://github.com/apache/parquet-format) | [Snowflake/Parquet](https://www.snowflake.com/en/fundamentals/parquet/)

# %%
print('--- Benchmarking Parquet ---')

for name, comp in [(SEM_COMPRESSAO, 'uncompressed'), ('snappy', 'snappy'), ('zstd', 'zstd')]:
    file_path = f"output_formats/data_{name}.parquet"

    mem_start = proc.memory_info().rss
    w_time = _t(lambda: df_polars.write_parquet(file_path, compression=comp))
    w_mem = max(0.1, (proc.memory_info().rss - mem_start) / 1e6)

    r_time = _t(lambda: pl.read_parquet(file_path))
    m_time = _t(lambda: pq.read_metadata(file_path))
    c_time = _t(lambda: pl.read_parquet(file_path, columns=['codigo_municipio', 'populacao_total']))

    results.append({"Format": "Parquet", "Compression": name,
        "Write Time (s)": w_time, "Read Time (s)": r_time,
        "Meta Time (s)": m_time, "Col Time (s)": c_time,
        "Size (MB)": get_size_mb(file_path), "Peak RAM (MB)": w_mem})
    print(f"✔️ Parquet {name} OK")


# %% [markdown]
# **ORC (Optimized Row Columnar)** — Formato colunar nascido no Apache Hive v0.11 (Hortonworks/Cloudera) para superar as limitações cegas do RCFile legado. Introduziu consciência semântica de tipos para habilitar compressão analítica especializada.
# 
# | Aspecto | Detalhe |
# |---|---|
# | **Estrutura** | Stripes de 64–256 MB com Row Group Indexes por bloco de 10k linhas; File Tail com estatísticas globais |
# | **Encoding** | Dictionary para strings; ZigZag+Varint para inteiros; Boolean/Byte RLE nativo |
# | **Compressão** | Interna (bit-packing, RLE) + externa plugável: Zlib (padrão), Snappy, LZ4, ZSTD |
# | **Ponto forte** | Bloom Filters + min/max/sum integrados → Predicate Pushdown altamente eficiente |
# | **Ponto fraco** | Menor ecossistema fora do Hive em comparação ao Parquet |
# 
# **Referências:** [ORC Spec v1](https://orc.apache.org/specification/ORCv1/) | [ORC Spec v2](https://orc.apache.org/specification/ORCv2/) | [RCFile vs ORC](https://www.bigdatainrealworld.com/rcfile-vs-orc/)

# %%
print('--- Benchmarking ORC ---')
table = pa.Table.from_pandas(df_pandas)

for name, comp in [(SEM_COMPRESSAO, 'uncompressed'), ('snappy', 'snappy')]:
    file_path = f"output_formats/data_{name}.orc"

    mem_start = proc.memory_info().rss
    w_time = _t(lambda: orc.write_table(table, file_path, compression=comp))
    w_mem = max(0.1, (proc.memory_info().rss - mem_start) / 1e6)

    r_time = _t(lambda: orc.read_table(file_path))
    m_time = _t(lambda: orc.ORCFile(file_path).schema)
    c_time = _t(lambda: orc.read_table(file_path, columns=['codigo_municipio', 'populacao_total']))

    results.append({"Format": "ORC", "Compression": name,
        "Write Time (s)": w_time, "Read Time (s)": r_time,
        "Meta Time (s)": m_time, "Col Time (s)": c_time,
        "Size (MB)": get_size_mb(file_path), "Peak RAM (MB)": w_mem})
    print(f"✔️ ORC {name} OK")


# %% [markdown]
# **Apache Arrow** — Formato in-memory colunar criado por Wes McKinney (Pandas/Dremio) para eliminar o overhead de serialização (marshaling/unmarshaling) entre sistemas heterogêneos (Python ↔ JVM/Spark ↔ Rust) com acesso zero-copy.
# 
# | Aspecto | Detalhe |
# |---|---|
# | **Estrutura** | Arrays alinhados em 64 bytes (SIMD-ready via Intel AVX-512); metadados via Google Flatbuffers |
# | **Encoding** | Fixed-Size Primitive; Variable-Size Binary com offset arrays 32/64-bit; Validity Bitmaps para nulos |
# | **Compressão** | Mínima por design (Dictionary leve, REE Run-End Encoded); compressão pesada anula zero-copy |
# | **Ponto forte** | Acesso O(1) a primitivos; IPC entre processos sem desserialização; CPU Cache Locality |
# | **Ponto fraco** | Não substitui Parquet para armazenamento persistente e comprimido em disco |
# 
# **Referências:** [Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html) | [Arrow FAQ](https://arrow.apache.org/faq/) | [Arrow + Parquet Python](https://arrow.apache.org/docs/python/parquet.html)

# %%
print('--- Benchmarking Arrow ---')

for name, comp in [(SEM_COMPRESSAO, 'uncompressed'), ('zstd', 'zstd')]:
    file_path = f"output_formats/data_{name}.arrow"

    mem_start = proc.memory_info().rss
    w_time = _t(lambda: df_polars.write_ipc(file_path, compression=comp))
    w_mem = max(0.1, (proc.memory_info().rss - mem_start) / 1e6)

    r_time = _t(lambda: pl.read_ipc(file_path))

    def _meta():
        with pa.OSFile(file_path, 'rb') as f:
            try: pa.ipc.open_file(f).schema
            except: pass
    m_time = _t(_meta)

    c_time = _t(lambda: pl.read_ipc(file_path, columns=['codigo_municipio', 'populacao_total']))

    results.append({"Format": "Arrow", "Compression": name,
        "Write Time (s)": w_time, "Read Time (s)": r_time,
        "Meta Time (s)": m_time, "Col Time (s)": c_time,
        "Size (MB)": get_size_mb(file_path), "Peak RAM (MB)": w_mem})
    print(f"✔️ Arrow {name} OK")


# %% [markdown]
# **2 - Baseados em Linha**

# %% [markdown]
# **Apache Avro** — Formato orientado a linha criado no ecossistema Hadoop por Doug Cutting. Projetado para substituir serializadores dependentes de geração de código (Protocol Buffers, Thrift). Padrão em pipelines de mensageria com Apache Kafka, microsserviços e AWS Glue Schema Registry.
# 
# | Aspecto | Detalhe |
# |---|---|
# | **Estrutura** | Schema JSON no cabeçalho único; registros binários em sequência na cauda do arquivo |
# | **Encoding** | Binário nativo; ZigZag+Varint para inteiros pequenos; JSON usado apenas para debug |
# | **Compressão** | Compressão genérica por bloco (Snappy, Deflate); não explora repetições colunares |
# | **Ponto forte** | Schema Evolution — evolução do esquema sem reescrita de dados históricos |
# | **Ponto fraco** | Layout row-oriented limita ganhos de compressão colunar |
# 
# **Referências:** [Especificação Avro](https://avro.apache.org/docs/current/specification/) | [Airbyte — O que é Avro](https://airbyte.com/data-engineering-resources/what-is-avro) | [Wikipedia Apache Avro](https://en.wikipedia.org/wiki/Apache_Avro)

# %%
print('--- Benchmarking Avro ---')

for name, comp in [(SEM_COMPRESSAO, None), ('snappy', 'snappy')]:
    file_path = f"output_formats/data_{name}.avro"

    mem_start = proc.memory_info().rss
    w_time = _t(lambda: df_polars.write_avro(file_path, compression=comp))
    w_mem = max(0.1, (proc.memory_info().rss - mem_start) / 1e6)

    r_time = _t(lambda: pl.read_avro(file_path))
    m_time = _t(lambda: pl.read_avro(file_path, n_rows=0))
    c_time = _t(lambda: pl.read_avro(file_path, columns=['codigo_municipio', 'populacao_total']))

    results.append({"Format": "Avro", "Compression": name,
        "Write Time (s)": w_time, "Read Time (s)": r_time,
        "Meta Time (s)": m_time, "Col Time (s)": c_time,
        "Size (MB)": get_size_mb(file_path), "Peak RAM (MB)": w_mem})
    print(f"✔️ Avro {name} OK")


# %%
print('--- Benchmarking Formatos Spark (RC, CarbonData, SequenceFile) ---')
import shutil
print(f'Shape Spark Formats: ({df_spark.count()}, {len(df_spark.columns)})')

formats = [
    ("SequenceFile", "sequenceFile"),
    ("RC", "rcfile"),
    ("CarbonData", "carbondata")
]

for name, fmt_string in formats:
    print(f"Testando Spark nativo para {name}...")
    file_path = f"output_formats/data_{name}"
    shutil.rmtree(file_path, ignore_errors=True)
    
    mem_start = proc.memory_info().rss
    start_time = time.time()
    
    try:
        # --- ESCRITA OFICIAL SPARk/HIVE ---
        if fmt_string == "sequenceFile":
            # O formato pair do Hadoop (Sequence) requer mapeamento RDD explicito
            df_spark.rdd.map(lambda r: (str(r[0]), str(r))).saveAsSequenceFile(file_path)
        elif fmt_string == "rcfile":
            # No modo Hive, o Spark usa a sintaxe de 'row format' mas podemos simular via saveAsTable ou format native se disponivel
            df_spark.write.format("hive").option("fileFormat", "rcfile").mode('overwrite').save(file_path)
        else:
            # CarbonData
            df_spark.write.format(fmt_string).mode('overwrite').save(file_path)
            
        print(f"    ✔️ {name} Escrito com sucesso!")
    except Exception as e:
        # Se falhar aqui, o Spark nao tem as bibliotecas Jars/Hadoop no classpath
        print(f"    [Aviso] Falha de suporte nativo no Spark local para {name}")
        
    w_time = time.time() - start_time
    w_mem = max(0.1, (proc.memory_info().rss - mem_start) / 1e6)
    
    # --- LEITURA ---
    start_time = time.time()
    try: 
        if fmt_string == "sequenceFile": _ = spark.sparkContext.sequenceFile(file_path).count()
        else: _ = spark.read.format(fmt_string).load(file_path).count()
    except: pass
    r_time = time.time() - start_time
    
    # --- METADADOS ---
    start_time = time.time()
    try: 
        if fmt_string == "sequenceFile": _ = spark.sparkContext.sequenceFile(file_path).first()
        else: _ = spark.read.format(fmt_string).load(file_path).schema
    except: pass
    m_time = time.time() - start_time
    
    # --- COLUNAS SELECIONADAS ---
    start_time = time.time()
    try: 
        if fmt_string != "sequenceFile":
            _ = spark.read.format(fmt_string).load(file_path).select('codigo_municipio', 'populacao_total').count()
    except: pass
    c_time = time.time() - start_time
    
    results.append({"Format": name, "Compression": SEM_COMPRESSAO, "Write Time (s)": w_time, "Read Time (s)": r_time,
                     "Meta Time (s)": m_time, "Col Time (s)": c_time, "Size (MB)": get_size_mb(file_path), "Peak RAM (MB)": w_mem})
    print(f"✔️ Processamento {name} Finalizado")


# %%
df_final = pd.DataFrame(results)
display(df_final)


# %%
plt.figure(figsize=(14, 6))
metrics_df = df_final.melt(id_vars=['Format', 'Compression'], value_vars=['Write Time (s)',
                                                                           'Read Time (s)'], var_name='Métrica', value_name='Tempo Total')
sns.barplot(data=metrics_df, x='Format', y='Tempo Total', hue='Métrica')
plt.title('Comparação de Tempo: Escrita vs Leitura')
plt.yscale('log') 
plt.xticks(rotation=45)
plt.show()


# %%
plt.figure(figsize=(14, 6))
read_metrics = df_final.melt(id_vars=['Format', 'Compression'], value_vars=['Read Time (s)', 'Col Time (s)',
                                                                             'Meta Time (s)'], var_name='Métrica', value_name='Tempo Específico')
sns.barplot(data=read_metrics, x='Format', y='Tempo Específico', hue='Métrica')
plt.title('Leitura Completa vs Leitura de Subset de Colunas vs Leitura de Metadados')
plt.yscale('log')
plt.xticks(rotation=45)
plt.show()


# %%
ax = df_final.pivot(index='Format', columns='Compression', values='Size (MB)').plot(kind='bar', figsize=(14, 6))
plt.title('Tamanho do Arquivo por Método de Compressão')
plt.ylabel('Size (MB)')
plt.yscale('log')
plt.xticks(rotation=45)
plt.show()


# %%
plt.figure(figsize=(14, 6))
sns.barplot(data=df_final, x='Format', y='Peak RAM (MB)')
plt.title('Pico de Uso de Memória RAM')
plt.xticks(rotation=45)
plt.show()


# %%
print(f"**Tempo Total de Processamento: {time.time() - workflow_start:.2f}s**")



