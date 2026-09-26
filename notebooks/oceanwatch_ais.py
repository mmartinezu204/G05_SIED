# Databricks notebook source
# MAGIC %md
# MAGIC # OceanWatch — AIS junio 2023 (1-7)
# MAGIC
# MAGIC Notebook único que reemplaza a `00_setup_ingesta`, `01_perfilamiento_calidad` y
# MAGIC `02_preguntas_negocio`.
# MAGIC
# MAGIC La descarga, el chunking y la subida al volumen **no** se hacen aquí: las hace
# MAGIC `setup.sh` (Terraform crea `oceanwatch.bronze.landing` y `databricks fs cp` sube
# MAGIC los `AIS_2023_06_DD_part_NNN.csv.gz`). Este notebook arranca desde el volumen,
# MAGIC materializa **una sola vez** la tabla Delta `bronze.ais_raw` y todo el análisis
# MAGIC posterior lee de esa tabla en vez de volver a parsear los CSV.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parámetros e imports
# MAGIC
# MAGIC Los defaults coinciden con lo que crea Terraform (`var.catalog_name` y el output
# MAGIC `volume_path.landing`). Si se corre como job, se sobreescriben vía `base_parameters`.

# COMMAND ----------

import glob
import gzip
import os
import re
import time

from pyspark.sql import Window
from pyspark.sql import functions as f

dbutils.widgets.text("catalog", "oceanwatch")
dbutils.widgets.text("landing_path", "/Volumes/oceanwatch/bronze/landing")
dbutils.widgets.dropdown("recrear_bronze", "false", ["true", "false"])

CATALOG = dbutils.widgets.get("catalog")
LANDING = dbutils.widgets.get("landing_path")
RECREAR_BRONZE = dbutils.widgets.get("recrear_bronze") == "true"

BRONZE_TABLE = f"{CATALOG}.bronze.ais_raw"
FILE_GLOB = "AIS_2023_06_*.csv.gz"

print("CATALOG:", CATALOG)
print("LANDING:", LANDING)
print("BRONZE_TABLE:", BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validación de archivos en el volumen
# MAGIC
# MAGIC Chequeos baratos hechos desde el driver sobre el FUSE de `/Volumes`: solo se
# MAGIC listan archivos y se lee la primera línea de cada `.gz`, no se escanea el corpus.

# COMMAND ----------

landing_files = sorted(
    glob.glob(f"{LANDING}/**/{FILE_GLOB}", recursive=True)
)

print(f"Chunks encontrados: {len(landing_files)}")

assert len(landing_files) > 0, (
    f"No hay archivos {FILE_GLOB} en {LANDING}. ¿Se corrió setup.sh?"
)

# COMMAND ----------

chunks_por_dia = {}

for path in landing_files:
    dia = re.search(r"AIS_(\d{4}_\d{2}_\d{2})_part_", os.path.basename(path)).group(1)
    chunks_por_dia.setdefault(dia, []).append(path)

for dia, paths in sorted(chunks_por_dia.items()):
    size_mb = sum(os.path.getsize(p) for p in paths) / 1024**2
    print(f"{dia}: {len(paths)} chunks, {size_mb:.1f} MB comprimidos")

expected_days = {f"2023_06_{day:02d}" for day in range(1, 8)}

missing_days = expected_days - set(chunks_por_dia)

assert not missing_days, f"Faltan días en el volumen: {missing_days}"

# COMMAND ----------

headers = {}

for path in landing_files:
    with gzip.open(path, "rt", encoding="utf-8") as file:
        headers[path] = file.readline().strip()

unique_headers = set(headers.values())

print("Cantidad de encabezados diferentes:", len(unique_headers))

for header in unique_headers:
    print(header)

assert len(unique_headers) == 1, (
    "Los chunks no comparten el mismo encabezado"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingesta: volumen → Delta `bronze.ais_raw`
# MAGIC
# MAGIC Se escribe solo si la tabla no existe o si `recrear_bronze = true`.
# MAGIC
# MAGIC Cada `.csv.gz` es leído por una sola task (gzip no es splittable); por eso
# MAGIC `setup.sh` los parte en chunks de ~100MB antes de comprimir.

# COMMAND ----------

AIS_SCHEMA = """
    MMSI string,
    BaseDateTime timestamp,
    LAT double,
    LON double,
    SOG float,
    COG float,
    Heading float,
    VesselName string,
    IMO string,
    CallSign string,
    VesselType smallint,
    Status smallint,
    Length float,
    Width float,
    Draft float,
    Cargo string,
    TransceiverClass string
"""

if RECREAR_BRONZE or not spark.catalog.tableExists(BRONZE_TABLE):

    ais_csv = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", FILE_GLOB)
        .schema(AIS_SCHEMA)
        .load(LANDING)
        .withColumn("fecha", f.to_date("BaseDateTime"))
        .withColumn("archivo_origen", f.col("_metadata.file_name"))
        .withColumn(
            "fecha_archivo",
            f.to_date(
                f.regexp_extract(
                    f.col("_metadata.file_name"),
                    r"AIS_(\d{4}_\d{2}_\d{2})_part_",
                    1
                ),
                "yyyy_MM_dd"
            )
        )
        .withColumn("_ingested_at", f.current_timestamp())
    )

    inicio = time.perf_counter()

    (
        ais_csv
        .write
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(BRONZE_TABLE)
    )

    print(f"{BRONZE_TABLE} escrita en {time.perf_counter() - inicio:.2f} segundos")

else:
    print(f"{BRONZE_TABLE} ya existe; se reutiliza (recrear_bronze = false)")

# COMMAND ----------

ais = spark.table(BRONZE_TABLE)

ais.printSchema()

display(ais.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validaciones de la carga

# COMMAND ----------

archivos_en_tabla = (
    ais
    .select("archivo_origen")
    .distinct()
    .count()
)

print(f"Archivos distintos en la tabla: {archivos_en_tabla}")

assert archivos_en_tabla == len(landing_files), (
    f"La tabla tiene {archivos_en_tabla} archivos de origen "
    f"y el volumen {len(landing_files)}"
)

# COMMAND ----------

expected_dates = {
    f"2023-06-{day:02d}"
    for day in range(1, 8)
}

actual_dates = {
    str(row["fecha"])
    for row in ais.select("fecha").distinct().collect()
}

print("Fechas encontradas:", sorted(actual_dates))

assert actual_dates == expected_dates

# COMMAND ----------

# MAGIC %md
# MAGIC # Parte I — Perfilamiento y calidad

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Posiciones y buques únicos por día

# COMMAND ----------

perfil_diario = (
    ais
    .groupBy("fecha")
    .agg(
        f.count("*").alias("posiciones"),
        f.countDistinct("MMSI").alias("buques_unicos")
    )
    .orderBy("fecha")
)

display(perfil_diario)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total semanal

# COMMAND ----------

resumen_general = (
    ais
    .agg(
        f.count("*").alias("total_posiciones"),
        f.countDistinct("MMSI").alias("buques_unicos_semana"),
        f.min("BaseDateTime").alias("primer_timestamp"),
        f.max("BaseDateTime").alias("ultimo_timestamp")
    )
)

display(resumen_general)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Completitud

# COMMAND ----------

columnas_originales = [
    "MMSI",
    "BaseDateTime",
    "LAT",
    "LON",
    "SOG",
    "COG",
    "Heading",
    "VesselName",
    "IMO",
    "CallSign",
    "VesselType",
    "Status",
    "Length",
    "Width",
    "Draft",
    "Cargo",
    "TransceiverClass"
]

string_cols = {
    field.name
    for field in ais.schema.fields
    if field.dataType.simpleString() == "string"
}

missing_exprs = []

for col_name in columnas_originales:

    if col_name in string_cols:
        missing_condition = (
            f.col(col_name).isNull()
            | (f.trim(f.col(col_name)) == "")
        )
    else:
        missing_condition = f.col(col_name).isNull()

    missing_exprs.append(
        f.sum(
            f.when(missing_condition, 1).otherwise(0)
        ).alias(col_name)
    )

missing_row = (
    ais
    .agg(
        f.count("*").alias("total_filas"),
        *missing_exprs
    )
    .first()
)

total_filas = missing_row["total_filas"]

perfil_completitud = spark.createDataFrame(
    [
        (
            col_name,
            int(missing_row[col_name]),
            round(
                100 * missing_row[col_name] / total_filas,
                4
            )
        )
        for col_name in columnas_originales
    ],
    [
        "columna",
        "valores_faltantes",
        "porcentaje_faltante"
    ]
)

display(
    perfil_completitud
    .orderBy(f.desc("porcentaje_faltante"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distribución por tipo de buque

# COMMAND ----------

distribucion_tipo = (
    ais
    .groupBy("VesselType")
    .agg(
        f.count("*").alias("posiciones"),
        f.countDistinct("MMSI").alias("buques_unicos")
    )
    .withColumn(
        "porcentaje_posiciones",
        f.round(
            100
            * f.col("posiciones")
            / f.lit(total_filas),
            4
        )
    )
    .orderBy(f.desc("posiciones"))
)

display(distribucion_tipo)

# COMMAND ----------

display(
    distribucion_tipo
    .filter(f.col("VesselType").isNull())
)

# COMMAND ----------

display(distribucion_tipo.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Distribución por tamaño

# COMMAND ----------

dimensiones_validas = (
    ais
    .select(
        f.when(f.col("Length") > 0, f.col("Length")).alias("Length"),
        f.when(f.col("Width") > 0, f.col("Width")).alias("Width"),
        f.when(f.col("Draft") > 0, f.col("Draft")).alias("Draft")
    )
)

display(
    dimensiones_validas.summary(
        "count",
        "mean",
        "stddev",
        "min",
        "25%",
        "50%",
        "75%",
        "90%",
        "95%",
        "99%",
        "max"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revisión dimensiones extremas

# COMMAND ----------

columnas_dimensiones = [
    "MMSI",
    "VesselName",
    "VesselType",
    "Length",
    "Width",
    "Draft"
]

buques_dimensiones = (
    ais
    .select(*columnas_dimensiones)
    .dropDuplicates(columnas_dimensiones)
)

display(
    buques_dimensiones
    .orderBy(f.desc("Length"))
    .limit(50)
)

# COMMAND ----------

display(
    buques_dimensiones
    .orderBy(f.desc("Width"))
    .limit(50)
)

# COMMAND ----------

ais_tamano = (
    ais
    .withColumn(
        "categoria_tamano",
        f.when(
            f.col("Length").isNull() | (f.col("Length") <= 0),
            "Sin longitud utilizable"
        )
        .when(f.col("Length") < 25, "< 25 m")
        .when(f.col("Length") < 50, "25 - 49.9 m")
        .when(f.col("Length") < 100, "50 - 99.9 m")
        .when(f.col("Length") < 200, "100 - 199.9 m")
        .otherwise(">= 200 m")
    )
)

distribucion_tamano = (
    ais_tamano
    .groupBy("categoria_tamano")
    .agg(
        f.count("*").alias("posiciones"),
        f.countDistinct("MMSI").alias("buques_unicos")
    )
    .withColumn(
        "porcentaje_posiciones",
        f.round(
            100
            * f.col("posiciones")
            / f.lit(total_filas),
            4
        )
    )
    .orderBy(f.desc("posiciones"))
)

display(distribucion_tamano)

# COMMAND ----------

tipo_tamano = (
    ais_tamano
    .groupBy(
        "VesselType",
        "categoria_tamano"
    )
    .agg(
        f.count("*").alias("posiciones"),
        f.countDistinct("MMSI").alias("buques_unicos")
    )
    .orderBy(f.desc("posiciones"))
)

display(tipo_tamano.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Reglas de calidad

# COMMAND ----------

inicio_semana = f.lit("2023-06-01 00:00:00").cast("timestamp")
fin_semana = f.lit("2023-06-08 00:00:00").cast("timestamp")

sog_no_disponible = (
    f.abs(f.col("SOG") - f.lit(102.3)) < 0.001
)

checks = [
    (
        "coordenadas_nulas",
        f.col("LAT").isNull() | f.col("LON").isNull(),
        "LAT o LON no disponibles"
    ),
    (
        "lat_fuera_rango",
        (f.col("LAT") < -90) | (f.col("LAT") > 90),
        "Latitud fuera de [-90, 90]"
    ),
    (
        "lon_fuera_rango",
        (f.col("LON") < -180) | (f.col("LON") > 180),
        "Longitud fuera de [-180, 180]"
    ),

    (
        "sog_nulo",
        f.col("SOG").isNull(),
        "Velocidad no informada"
    ),
    (
        "sog_no_disponible_102_3",
        sog_no_disponible,
        "102.3 corresponde al valor AIS de SOG no disponible"
    ),
    (
        "sog_fuera_codificacion_ais",
        (f.col("SOG") < 0)
        | (
            (f.col("SOG") > 102.3)
            & (~sog_no_disponible)
        ),
        "Valor fuera del rango esperado para SOG AIS"
    ),
    (
        "sog_mayor_60_sospechosa",
        (f.col("SOG") > 60) & (f.col("SOG") <= 102.2),
        "Velocidad muy alta: revisar, no eliminar automáticamente"
    ),

    (
        "cog_no_disponible_360",
        f.abs(f.col("COG") - f.lit(360.0)) < 0.001,
        "COG=360 significa no disponible"
    ),
    (
        "cog_fuera_rango",
        (f.col("COG") < 0) | (f.col("COG") > 360),
        "COG fuera del rango AIS"
    ),

    (
        "heading_no_disponible_511",
        f.abs(f.col("Heading") - f.lit(511.0)) < 0.001,
        "Heading=511 significa no disponible"
    ),
    (
        "heading_fuera_rango",
        (f.col("Heading") < 0)
        | (
            (f.col("Heading") > 359)
            & (f.abs(f.col("Heading") - f.lit(511.0)) >= 0.001)
        ),
        "Heading distinto de 0-359 y no es el sentinel 511"
    ),

    (
        "mmsi_nulo_o_vacio",
        f.col("MMSI").isNull()
        | (f.trim(f.col("MMSI")) == ""),
        "MMSI ausente"
    ),
    (
        "mmsi_formato_invalido",
        f.col("MMSI").isNotNull()
        & (~f.trim(f.col("MMSI")).rlike(r"^[0-9]{9}$")),
        "MMSI que no contiene exactamente 9 dígitos"
    ),

    (
        "timestamp_nulo",
        f.col("BaseDateTime").isNull(),
        "Timestamp ausente"
    ),
    (
        "timestamp_fuera_semana",
        (f.col("BaseDateTime") < inicio_semana)
        | (f.col("BaseDateTime") >= fin_semana),
        "Timestamp fuera del corpus 1-7 junio"
    ),
    (
        "fecha_archivo_no_extraida",
        f.col("fecha_archivo").isNull(),
        "No se pudo extraer la fecha del nombre del archivo de origen"
    ),
    (
        "fecha_no_coincide_archivo",
        f.col("fecha").isNotNull()
        & f.col("fecha_archivo").isNotNull()
        & (f.col("fecha") != f.col("fecha_archivo")),
        "El timestamp no corresponde al día indicado por el archivo"
    ),

    (
        "length_negativo",
        f.col("Length") < 0,
        "Longitud físicamente inválida"
    ),
    (
        "width_negativo",
        f.col("Width") < 0,
        "Ancho físicamente inválido"
    ),
    (
        "draft_negativo",
        f.col("Draft") < 0,
        "Calado físicamente inválido"
    ),

    (
        "length_cero",
        f.col("Length") == 0,
        "Longitud cero; posiblemente no disponible"
    ),
    (
        "width_cero",
        f.col("Width") == 0,
        "Ancho cero; posiblemente no disponible"
    ),
    (
        "draft_cero",
        f.col("Draft") == 0,
        "Calado cero; posiblemente no disponible"
    ),

    (
        "vessel_type_nulo",
        f.col("VesselType").isNull(),
        "Tipo de buque no informado"
    )
]

# COMMAND ----------

quality_exprs = []

for i, (_, condition, _) in enumerate(checks):
    quality_exprs.append(
        f.sum(
            f.when(condition, 1).otherwise(0)
        ).alias(f"q_{i}")
    )

quality_row = (
    ais
    .agg(
        f.count("*").alias("total_filas"),
        *quality_exprs
    )
    .first()
)

total_filas = quality_row["total_filas"]

# COMMAND ----------

quality_data = []

for i, (nombre, _, descripcion) in enumerate(checks):

    cantidad = int(quality_row[f"q_{i}"])

    porcentaje = round(
        100 * cantidad / total_filas,
        6
    )

    quality_data.append(
        (
            nombre,
            cantidad,
            porcentaje,
            descripcion
        )
    )

diagnostico_calidad = spark.createDataFrame(
    quality_data,
    [
        "regla",
        "filas_afectadas",
        "porcentaje",
        "interpretacion"
    ]
)

display(
    diagnostico_calidad
    .orderBy(f.desc("filas_afectadas"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Headings y COG anómalos

# COMMAND ----------

heading_anomalos = (
    ais
    .filter(
        (f.col("Heading") < 0)
        | (
            (f.col("Heading") > 359)
            & (f.abs(f.col("Heading") - 511.0) >= 0.001)
        )
    )
    .groupBy("Heading")
    .agg(
        f.count("*").alias("apariciones")
    )
    .orderBy(
        f.desc("apariciones")
    )
)

display(heading_anomalos)

# COMMAND ----------

cog_anomalos = (
    ais
    .filter(
        (f.col("COG") < 0)
        | (f.col("COG") > 360)
    )
    .groupBy("COG")
    .agg(
        f.count("*").alias("apariciones")
    )
    .orderBy(
        f.desc("apariciones")
    )
)

display(cog_anomalos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Velocidades altas

# COMMAND ----------

display(
    ais
    .filter(
        (f.col("SOG") > 60)
        & (f.col("SOG") <= 102.2)
    )
    .select(
        "MMSI",
        "BaseDateTime",
        "LAT",
        "LON",
        "SOG",
        "VesselType",
        "VesselName"
    )
    .orderBy(f.desc("SOG"))
    .limit(100)
)

# COMMAND ----------

display(
    ais
    .select("SOG")
    .filter(f.col("SOG").isNotNull())
    .summary(
        "count",
        "mean",
        "stddev",
        "min",
        "50%",
        "90%",
        "95%",
        "99%",
        "max"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Duplicados
# MAGIC
# MAGIC Duplicado = mismo MMSI + BaseDateTime

# COMMAND ----------

duplicados_timestamp = (
    ais
    .filter(
        f.col("MMSI").isNotNull()
        & f.col("BaseDateTime").isNotNull()
    )
    .groupBy(
        "MMSI",
        "BaseDateTime"
    )
    .agg(
        f.count("*").alias("numero_registros"),
        f.countDistinct(
            f.struct("LAT", "LON")
        ).alias("posiciones_distintas")
    )
    .filter(
        f.col("numero_registros") > 1
    )
)

# COMMAND ----------

resumen_duplicados = (
    duplicados_timestamp
    .agg(
        f.count("*").alias(
            "claves_mmsi_timestamp_duplicadas"
        ),

        f.sum("numero_registros").alias(
            "filas_en_claves_duplicadas"
        ),

        f.sum(
            f.col("numero_registros") - 1
        ).alias(
            "filas_excedentes"
        ),

        f.sum(
            f.when(
                f.col("posiciones_distintas") == 1,
                1
            ).otherwise(0)
        ).alias(
            "claves_con_misma_posicion"
        ),

        f.sum(
            f.when(
                f.col("posiciones_distintas") > 1,
                1
            ).otherwise(0)
        ).alias(
            "claves_con_posiciones_conflictivas"
        )
    )
)

display(resumen_duplicados)

# COMMAND ----------

display(
    duplicados_timestamp
    .orderBy(
        f.desc("numero_registros")
    )
    .limit(100)
)

# COMMAND ----------

display(
    duplicados_timestamp
    .filter(
        f.col("posiciones_distintas") > 1
    )
    .orderBy(
        f.desc("numero_registros")
    )
    .limit(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicados por contenido completo

# COMMAND ----------

ais_hash = (
    ais
    .withColumn(
        "_row_hash",
        f.xxhash64(
            *[f.col(c) for c in columnas_originales]
        )
    )
)

duplicados_fila = (
    ais_hash
    .groupBy("_row_hash")
    .agg(
        f.count("*").alias("numero_registros")
    )
    .filter(
        f.col("numero_registros") > 1
    )
)

# COMMAND ----------

resumen_duplicados_fila = (
    duplicados_fila
    .agg(
        f.count("*").alias(
            "grupos_repetidos"
        ),
        f.sum("numero_registros").alias(
            "filas_en_grupos_repetidos"
        ),
        f.sum(
            f.col("numero_registros") - 1
        ).alias(
            "filas_excedentes"
        )
    )
)

display(resumen_duplicados_fila)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. MMSI anómalos

# COMMAND ----------

mmsi_anomalos = (
    ais
    .filter(
        f.col("MMSI").isNull()
        | (f.trim(f.col("MMSI")) == "")
        | (~f.trim(f.col("MMSI")).rlike(r"^[0-9]{9}$"))
    )
    .groupBy("MMSI")
    .agg(
        f.count("*").alias("posiciones")
    )
    .orderBy(
        f.desc("posiciones")
    )
)

display(mmsi_anomalos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Coordenadas anómalas

# COMMAND ----------

display(
    ais
    .filter(
        (f.col("LAT") < -90)
        | (f.col("LAT") > 90)
        | (f.col("LON") < -180)
        | (f.col("LON") > 180)
    )
    .groupBy(
        "LAT",
        "LON"
    )
    .agg(
        f.count("*").alias("apariciones")
    )
    .orderBy(
        f.desc("apariciones")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Problemas de calidad por día

# COMMAND ----------

calidad_por_dia = (
    ais
    .groupBy("fecha")
    .agg(
        f.count("*").alias("posiciones"),

        f.sum(
            f.when(
                (f.col("LAT") < -90)
                | (f.col("LAT") > 90)
                | (f.col("LON") < -180)
                | (f.col("LON") > 180),
                1
            ).otherwise(0)
        ).alias("coordenadas_fuera_rango"),

        f.sum(
            f.when(
                f.abs(f.col("SOG") - 102.3) < 0.001,
                1
            ).otherwise(0)
        ).alias("sog_no_disponible"),

        f.sum(
            f.when(
                f.col("MMSI").isNull()
                | (~f.trim(f.col("MMSI")).rlike(r"^[0-9]{9}$")),
                1
            ).otherwise(0)
        ).alias("mmsi_anomalo")
    )
    .orderBy("fecha")
)

display(calidad_por_dia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Conclusiones del perfilamiento
# MAGIC
# MAGIC El corpus analizado contiene **60.533.559 posiciones AIS** correspondientes a
# MAGIC **31.871 MMSI distintos**, registradas entre el 1 y el 7 de junio de 2023.
# MAGIC El volumen diario se mantiene entre aproximadamente 8,0 y 9,1 millones de
# MAGIC posiciones, sin observarse días ausentes dentro del periodo analizado.
# MAGIC
# MAGIC ### Completitud
# MAGIC
# MAGIC Las variables fundamentales para el análisis de posición presentan una alta
# MAGIC completitud. MMSI, BaseDateTime, LAT, LON, SOG, COG, Heading y
# MAGIC TransceiverClass no contienen valores nulos.
# MAGIC
# MAGIC La principal limitación de completitud se encuentra en atributos estáticos de
# MAGIC las embarcaciones:
# MAGIC
# MAGIC - Draft: 64,43% de valores faltantes.
# MAGIC - IMO: 42,77%.
# MAGIC - Status: 32,96%.
# MAGIC - Cargo: 32,87%.
# MAGIC - CallSign: 16,78%.
# MAGIC - Width: 15,23%.
# MAGIC - Length: 6,05%.
# MAGIC
# MAGIC Adicionalmente, algunos atributos dimensionales utilizan el valor cero, que no
# MAGIC resulta útil como dimensión física. Considerando tanto nulos como valores cero,
# MAGIC aproximadamente el 9,49% de las posiciones no cuenta con una longitud
# MAGIC utilizable, el 18,91% no cuenta con ancho utilizable y el 68,92% no cuenta con
# MAGIC calado utilizable.
# MAGIC
# MAGIC ### Validez
# MAGIC
# MAGIC No se encontraron coordenadas fuera de los rangos globales válidos de latitud
# MAGIC y longitud, posiciones con coordenadas nulas, timestamps fuera del periodo
# MAGIC analizado ni inconsistencias entre la fecha del mensaje y el archivo de origen.
# MAGIC
# MAGIC Tampoco se encontraron valores SOG fuera de la codificación AIS utilizada en
# MAGIC el dataset.
# MAGIC
# MAGIC Se identificaron, sin embargo, algunos valores anómalos:
# MAGIC
# MAGIC - 49.897 posiciones (0,0824%) presentan MMSI que no cumplen el formato
# MAGIC   esperado de nueve dígitos.
# MAGIC - 1.061 posiciones (0,00175%) presentan Heading fuera del rango esperado y
# MAGIC   diferente del valor sentinel 511.
# MAGIC - 12 posiciones presentan COG superior al rango esperado.
# MAGIC - 626 posiciones presentan velocidades superiores a 60 nudos y se consideran
# MAGIC   sospechosas para revisión, aunque no se clasifican automáticamente como
# MAGIC   inválidas.
# MAGIC
# MAGIC ### Valores no disponibles definidos por AIS
# MAGIC
# MAGIC Se encontraron valores especiales del estándar que representan información no
# MAGIC disponible y, por lo tanto, no deben confundirse con errores de calidad:
# MAGIC
# MAGIC - Heading = 511: 33.535.628 posiciones (55,40%).
# MAGIC - COG = 360: 10.291.131 posiciones (17,00%).
# MAGIC - SOG = 102.3: 159.987 posiciones (0,264%).
# MAGIC
# MAGIC Estos valores deberán ser tratados de manera diferenciada en etapas posteriores
# MAGIC de limpieza y transformación.
# MAGIC
# MAGIC ### Duplicados
# MAGIC
# MAGIC Se identificaron **1.672 combinaciones MMSI + BaseDateTime repetidas**, que
# MAGIC involucran 3.344 registros y representan 1.672 filas excedentes.
# MAGIC
# MAGIC De estas claves repetidas:
# MAGIC
# MAGIC - 1.400 presentan la misma posición.
# MAGIC - 272 presentan posiciones distintas para el mismo MMSI y timestamp, lo que
# MAGIC   constituye una inconsistencia que requiere tratamiento específico.
# MAGIC
# MAGIC Al comparar el contenido completo de los registros mediante un hash de fila se
# MAGIC encontraron **1.388 grupos repetidos**, correspondientes a 2.776 filas y
# MAGIC 1.388 filas excedentes por contenido.
# MAGIC
# MAGIC Esto indica que no todos los registros que comparten MMSI, timestamp y posición
# MAGIC son duplicados completos; algunos presentan diferencias en otros atributos.
# MAGIC
# MAGIC ### Conclusión
# MAGIC
# MAGIC El dataset presenta una calidad adecuada para el análisis de tráfico marítimo
# MAGIC en sus variables principales de posición y tiempo, pero contiene problemas
# MAGIC relevantes en los atributos estáticos de los buques, identificadores MMSI,
# MAGIC valores especiales de navegación y duplicados.
# MAGIC
# MAGIC El diagnóstico realizado en esta etapa servirá como base para definir las
# MAGIC reglas de limpieza, normalización y control de calidad de la siguiente fase
# MAGIC del proyecto.

# COMMAND ----------

# MAGIC %md
# MAGIC # Parte II — Preguntas de negocio

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Bases analíticas mínimas

# COMMAND ----------

mmsi_valido = (
    f.col("MMSI").isNotNull()
    & f.trim(f.col("MMSI")).rlike(r"^[0-9]{9}$")
)

coordenada_valida = (
    f.col("LAT").between(-90, 90)
    & f.col("LON").between(-180, 180)
)

ais_buques = (
    ais
    .filter(mmsi_valido)
)

ais_espacial = (
    ais
    .filter(coordenada_valida)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. ¿Cuántos buques distintos transmitieron cada día?
# MAGIC
# MAGIC Se compara el resultado exacto obtenido mediante `countDistinct` con
# MAGIC `approx_count_distinct`.
# MAGIC
# MAGIC Para esta pregunta se consideran únicamente MMSI con formato válido de nueve
# MAGIC dígitos, ya que el perfilamiento identificó identificadores anómalos que no
# MAGIC deben interpretarse automáticamente como buques distintos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conteo exacto

# COMMAND ----------

exactos = (
    ais_buques
    .groupBy("fecha")
    .agg(
        f.countDistinct("MMSI").alias("buques_exactos")
    )
    .orderBy("fecha")
)

print("PLAN CONTEO EXACTO")
exactos.explain("formatted")

# COMMAND ----------

inicio = time.perf_counter()

exact_rows = exactos.collect()

tiempo_exacto = time.perf_counter() - inicio

print(f"Tiempo conteo exacto: {tiempo_exacto:.2f} segundos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conteo aproximado

# COMMAND ----------

aproximados = (
    ais_buques
    .groupBy("fecha")
    .agg(
        f.approx_count_distinct("MMSI").alias("buques_aproximados")
    )
    .orderBy("fecha")
)

print("PLAN CONTEO APROXIMADO")
aproximados.explain("formatted")

# COMMAND ----------

inicio = time.perf_counter()

approx_rows = aproximados.collect()

tiempo_aproximado = time.perf_counter() - inicio

print(f"Tiempo conteo aproximado: {tiempo_aproximado:.2f} segundos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparación

# COMMAND ----------

exact_map = {
    row["fecha"]: row["buques_exactos"]
    for row in exact_rows
}

approx_map = {
    row["fecha"]: row["buques_aproximados"]
    for row in approx_rows
}

comparacion_data = []

for fecha in sorted(exact_map.keys()):

    exacto = exact_map[fecha]
    aproximado = approx_map[fecha]

    error_abs = abs(aproximado - exacto)

    error_pct = (
        100 * error_abs / exacto
        if exacto > 0
        else 0
    )

    comparacion_data.append(
        (
            fecha,
            exacto,
            aproximado,
            error_abs,
            float(error_pct)
        )
    )

comparacion_a = spark.createDataFrame(
    comparacion_data,
    [
        "fecha",
        "buques_exactos",
        "buques_aproximados",
        "error_absoluto",
        "error_porcentual"
    ]
)

display(comparacion_a)

display(
    comparacion_a
    .agg(
        f.round(
            f.avg("error_porcentual"),
            4
        ).alias("error_porcentual_medio"),

        f.round(
            f.max("error_porcentual"),
            4
        ).alias("error_porcentual_maximo")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. ¿Qué tipos de buque generan más tráfico?
# MAGIC
# MAGIC Se calcula el Top 10 de códigos de tipo de buque según el número de posiciones
# MAGIC AIS transmitidas durante la semana.
# MAGIC
# MAGIC Los códigos se enriquecen utilizando el catálogo oficial de VesselType de
# MAGIC Marine Cadastre. [catalogo](https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf)
# MAGIC
# MAGIC Para la velocidad media se excluye SOG=102.3 porque el perfilamiento determinó
# MAGIC que representa velocidad no disponible y no una velocidad real.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catálogo de tipos

# COMMAND ----------

catalogo = []

def agregar(codigo, grupo, descripcion):
    catalogo.append(
        (codigo, grupo, descripcion)
    )


# 0
agregar(
    0,
    "Not Available",
    "Not available or no ship, default"
)

# 1-19
for c in range(1, 20):
    agregar(
        c,
        "Other",
        "Reserved for future use"
    )


# 20-29: WIG
wig = {
    20: ("Other", "Wing in ground (WIG), all ships of this type"),
    21: ("Tug Tow", "Wing in ground (WIG), hazardous category A"),
    22: ("Tug Tow", "Wing in ground (WIG), hazardous category B"),
    23: ("Other", "Wing in ground (WIG), hazardous category C"),
    24: ("Other", "Wing in ground (WIG), hazardous category D"),
    25: ("Other", "Wing in ground (WIG), reserved for future use"),
    26: ("Other", "Wing in ground (WIG), reserved for future use"),
    27: ("Other", "Wing in ground (WIG), reserved for future use"),
    28: ("Other", "Wing in ground (WIG), reserved for future use"),
    29: ("Other", "Wing in ground (WIG), reserved for future use"),
}

for codigo, (grupo, descripcion) in wig.items():
    agregar(codigo, grupo, descripcion)


# 30-59
tipos_especiales = {
    30: ("Fishing", "Fishing"),
    31: ("Tug Tow", "Towing"),
    32: ("Tug Tow", "Towing: length exceeds 200m or breadth exceeds 25m"),
    33: ("Other", "Dredging or underwater operations"),
    34: ("Other", "Diving operations"),
    35: ("Military", "Military operations"),
    36: ("Pleasure Craft/Sailing", "Sailing"),
    37: ("Pleasure Craft/Sailing", "Pleasure Craft"),
    38: ("Other", "Reserved"),
    39: ("Other", "Reserved"),

    40: ("Other", "High speed craft (HSC), all ships of this type"),
    41: ("Other", "High speed craft (HSC), hazardous category A"),
    42: ("Other", "High speed craft (HSC), hazardous category B"),
    43: ("Other", "High speed craft (HSC), hazardous category C"),
    44: ("Other", "High speed craft (HSC), hazardous category D"),
    45: ("Other", "High speed craft (HSC), reserved for future use"),
    46: ("Other", "High speed craft (HSC), reserved for future use"),
    47: ("Other", "High speed craft (HSC), reserved for future use"),
    48: ("Other", "High speed craft (HSC), reserved for future use"),
    49: ("Other", "High speed craft (HSC), no additional information"),

    50: ("Other", "Pilot Vessel"),
    51: ("Other", "Search and Rescue vessel"),
    52: ("Tug Tow", "Tug"),
    53: ("Other", "Port Tender"),
    54: ("Other", "Anti-pollution equipment"),
    55: ("Other", "Law Enforcement"),
    56: ("Other", "Spare - for assignment to local vessel"),
    57: ("Other", "Spare - for assignment to local vessel"),
    58: ("Other", "Medical Transport"),
    59: ("Other", "Ship according to RR Resolution No. 18"),
}

for codigo, (grupo, descripcion) in tipos_especiales.items():
    agregar(codigo, grupo, descripcion)

# COMMAND ----------

familias = {
    60: ("Passenger", "Passenger"),
    70: ("Cargo", "Cargo"),
    80: ("Tanker", "Tanker"),
    90: ("Other", "Other Type")
}

sufijos = {
    0: "all ships of this type",
    1: "hazardous category A",
    2: "hazardous category B",
    3: "hazardous category C",
    4: "hazardous category D",
    5: "reserved for future use",
    6: "reserved for future use",
    7: "reserved for future use",
    8: "reserved for future use",
    9: "no additional information"
}

for base, (grupo, nombre) in familias.items():

    for offset in range(10):

        codigo = base + offset

        agregar(
            codigo,
            grupo,
            f"{nombre}, {sufijos[offset]}"
        )

# COMMAND ----------

for c in range(100, 200):
    agregar(
        c,
        "Other",
        "Reserved for regional use"
    )

for c in range(200, 256):
    agregar(
        c,
        "Other",
        "Reserved for future use"
    )

for c in range(256, 1000):
    agregar(
        c,
        "Other",
        "No designation"
    )

# COMMAND ----------

avis = {
    1001: ("Fishing", "Commercial Fishing Vessel"),
    1002: ("Fishing", "Fish Processing Vessel"),
    1003: ("Cargo", "Freight Barge"),
    1004: ("Cargo", "Freight Ship"),
    1005: ("Other", "Industrial Vessel"),
    1006: ("Other", "Miscellaneous Vessel"),
    1007: ("Other", "Mobile Offshore Drilling Unit"),
    1008: ("Other", "Non-vessel"),
    1009: ("Other", "NON-VESSEL"),
    1010: ("Other", "Offshore Supply Vessel"),
    1011: ("Other", "Oil Recovery"),
    1012: ("Passenger", "Passenger (Inspected)"),
    1013: ("Passenger", "Passenger (Uninspected)"),
    1014: ("Passenger", "Passenger Barge (Inspected)"),
    1015: ("Passenger", "Passenger Barge (Uninspected)"),
    1016: ("Cargo", "Public Freight"),
    1017: ("Tanker", "Public Tankship/Barge"),
    1018: ("Other", "Public Vessel, Unclassified"),
    1019: ("Pleasure Craft/Sailing", "Recreational"),
    1020: ("Other", "Research Vessel"),
    1021: ("Military", "SAR Aircraft"),
    1022: ("Other", "School Ship"),
    1023: ("Tug Tow", "Tank Barge"),
    1024: ("Tanker", "Tank Ship"),
    1025: ("Tug Tow", "Towing Vessel")
}

for codigo, (grupo, descripcion) in avis.items():
    agregar(
        codigo,
        grupo,
        descripcion
    )

# COMMAND ----------

catalogo_tipos = spark.createDataFrame(
    catalogo,
    [
        "VesselType",
        "grupo_buque",
        "descripcion_tipo"
    ]
)

display(catalogo_tipos.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Respuesta

# COMMAND ----------

ais_velocidad = (
    ais
    .withColumn(
        "SOG_utilizable",
        f.when(
            f.col("SOG").between(0, 102.2),
            f.col("SOG")
        )
    )
)

# COMMAND ----------

top10_tipos_base = (
    ais_velocidad
    .filter(
        f.col("VesselType").isNotNull()
    )
    .groupBy("VesselType")
    .agg(
        f.count("*").alias("numero_posiciones"),

        f.round(
            f.avg("SOG_utilizable"),
            3
        ).alias("velocidad_media_nudos"),

        f.count("SOG_utilizable").alias(
            "posiciones_con_velocidad_utilizable"
        )
    )
    .orderBy(
        f.desc("numero_posiciones")
    )
    .limit(10)
)

# COMMAND ----------

resultado_b = (
    top10_tipos_base
    .join(
        f.broadcast(catalogo_tipos),
        on="VesselType",
        how="left"
    )
    .select(
        "VesselType",
        "grupo_buque",
        "descripcion_tipo",
        "numero_posiciones",
        "velocidad_media_nudos",
        "posiciones_con_velocidad_utilizable"
    )
    .orderBy(
        f.desc("numero_posiciones")
    )
)

display(resultado_b)

# COMMAND ----------

resultado_b.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. ¿Qué 10 buques recorrieron más distancia durante la semana?

# COMMAND ----------

window_vessel = Window.partitionBy("MMSI").orderBy("BaseDateTime")

df_lag = (ais
    .withColumn("LAT_prev", f.lag("LAT").over(window_vessel))
    .withColumn("LON_prev", f.lag("LON").over(window_vessel))
    .filter(f.col("LAT_prev").isNotNull() & f.col("LON_prev").isNotNull())
)

lat1 = f.radians(f.col("LAT_prev"))
lon1 = f.radians(f.col("LON_prev"))
lat2 = f.radians(f.col("LAT"))
lon2 = f.radians(f.col("LON"))

dlat = lat2 - lat1
dlon = lon2 - lon1

R_NM = 3440.0654

a = (f.sin(dlat / 2) ** 2) + f.cos(lat1) * f.cos(lat2) * (f.sin(dlon / 2) ** 2)
c = 2 * f.atan2(f.sqrt(a), f.sqrt(1 - a))
distancia_tramo = R_NM * c

df_distancias = (df_lag
    .withColumn("distancia_nm", distancia_tramo)
    .filter(f.col("distancia_nm") < 100)
)

top10_distancia = (df_distancias
    .groupBy("MMSI", "VesselName")
    .agg(
        f.round(f.sum("distancia_nm"), 2).alias("distancia_total_millas_nauticas"),
        f.round(f.try_divide(f.sum("distancia_nm"), f.avg("SOG")),2).alias("tiempo_total_horas")
    )
    .orderBy(f.col("distancia_total_millas_nauticas").desc())
    .limit(10)
)

display(top10_distancia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. ¿Dónde se concentra el tráfico?

# COMMAND ----------

df_h3 = ais.withColumn("h3_cell", f.expr("h3_longlatash3(LON, LAT, 8)"))

cells = (
    df_h3
    .groupBy("h3_cell")
    .agg(
        f.count("*").alias("num_posiciones"),
        f.round(f.avg("LAT"), 4).alias("lat_centroide"),
        f.round(f.avg("LON"), 4).alias("lon_centroide")
    )
    .orderBy(f.col("num_posiciones").desc())
    .limit(10)
)

display(cells)

# COMMAND ----------

# MAGIC %md
# MAGIC Falta lo de puertos

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. ¿Qué proporción de los buques de la semana transmitió los 7 días? ¿Dónde están los "visitantes de un solo día"?

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Proporción de buques que transmitieron los 7 días

# COMMAND ----------

df_dias_actividad = (ais
    .groupBy("MMSI")
    .agg(f.countDistinct("fecha").alias("dias_activos"))
)

proporcion_7_dias = (df_dias_actividad
    .select(
        f.count("MMSI").alias("total_buques"),
        f.sum(f.when(f.col("dias_activos") == 7, 1).otherwise(0)).alias("buques_7_dias"),
        f.sum(f.when(f.col("dias_activos") == 1, 1).otherwise(0)).alias("buques_1_dia")
    )
    .withColumn("porcentaje_7_dias", f.round((f.col("buques_7_dias") / f.col("total_buques")) * 100, 2))
    .withColumn("porcentaje_1_dia", f.round((f.col("buques_1_dia") / f.col("total_buques")) * 100, 2))
)

display(proporcion_7_dias)

# COMMAND ----------

# MAGIC %md
# MAGIC La proporción de buques que vistaron los 7 días es: $$ \frac{12667}{31871} $$
# MAGIC
# MAGIC Lo cual representa un 39.74% de todos los buques.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Ubicación de los visitantes de un solo día

# COMMAND ----------

mmsi_visitantes_1_dia = df_dias_actividad.filter(f.col("dias_activos") == 1).select("MMSI")

df_visitantes = ais.join(mmsi_visitantes_1_dia, on="MMSI", how="inner")

display(df_visitantes.limit(100))

# COMMAND ----------

ubicacion_visitantes = (df_visitantes
    .withColumn("h3_cell", f.expr("h3_longlatash3(LON, LAT, 8)"))
    .groupBy("h3_cell")
    .agg(
        f.count("*").alias("total_posiciones"),
        f.countDistinct("MMSI").alias("num_buques_visitantes"),
        f.round(f.avg("LAT"), 4).alias("lat_promedio"),
        f.round(f.avg("LON"), 4).alias("lon_promedio")
    )
    .orderBy(f.col("num_buques_visitantes").desc())
)

display(ubicacion_visitantes)
