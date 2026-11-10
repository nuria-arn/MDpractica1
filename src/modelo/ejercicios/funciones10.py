# uso de ChatGPT-6 para la corrección y optimización de estas funciones


from pyspark.sql.functions import *
from pyspark.sql.types import *
import yfinance as yf


def obtener_info_empresas(empresas):

    datos = []

    for ticker in empresas:
        info = yf.Ticker(ticker).info

        datos.append({
            "Ticker": ticker,
            "Nombre": info.get("longName") or info.get("shortName"),
            "Sector": info.get("sector"),
            "Industria": info.get("industry")
        })

    return datos

def guardar_dim_empresa_bronze(spark, datos):

    esquema = StructType([
        StructField("Ticker", StringType(), True),
        StructField("Nombre", StringType(), True),
        StructField("Sector", StringType(), True),
        StructField("Industria", StringType(), True)
    ])

    df = spark.createDataFrame(datos, esquema)

    df.write.format("delta").mode("overwrite").save(
        "data/lake/bronze/dim_empresa"
    )

    print("Filas dimensión empresa Bronze:", df.count())

    df.show(5, truncate=False)

    return df

def limpiar_dim_empresa(spark):

    df = spark.read.format("delta").load(
        "data/lake/bronze/dim_empresa"
    )

    print("Filas Bronze:", df.count())

    # Eliminar espacios sobrantes
    df = df.withColumn("Ticker", trim(col("Ticker")))
    df = df.withColumn("Nombre", trim(col("Nombre")))
    df = df.withColumn("Sector", lower(trim(col("Sector"))))
    df = df.withColumn("Industria", lower(trim(col("Industria"))))

    # Eliminar nulos
    df = df.dropna()

    # Eliminar duplicados
    df = df.dropDuplicates(["Ticker"])
    
    df.write.format("delta").mode("overwrite").save(
        "data/lake/silver/dim_empresa"
    )

    print("Filas Silver:", df.count())

    df.show(35, truncate=False)

    return df

def crear_dim_fecha(spark):

    df = spark.read.format("delta").load("data/lake/silver/ibex")

    # Obtener fechas
    dim_fecha = df.select(
        to_date(col("Date")).alias("Fecha")
    ).distinct()

    # Crear atributos temporales
    dim_fecha = dim_fecha.withColumn("Anio", year(col("Fecha"))) \
                         .withColumn("Mes", month(col("Fecha"))) \
                         .withColumn("Dia", dayofmonth(col("Fecha")))

    # Eliminar nulos
    dim_fecha = dim_fecha.dropna()

    # Guardar dimensión
    dim_fecha.write.format("delta").mode("overwrite").save(
        "data/lake/gold/dim_fecha"
    )

    print("Filas DIM_FECHA:", dim_fecha.count())
    dim_fecha.show(5, truncate=False)

    return dim_fecha
