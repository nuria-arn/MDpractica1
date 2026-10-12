import yfinance as yf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

def ejercicio2a(df):
    print("Ejercicio 2a:")
    df.printSchema()
    print("Las tres columnas aparecen de tipo string porque el csv no guarda tipos.")


def ejercicio2b(df, spark):
    print("Ejercicio 2b:")
    df.write.mode("overwrite").parquet("data/lake/bronze/sentimiento")
    df_sent = spark.read.parquet("data/lake/bronze/sentimiento")

    return df_sent

def ejercicio2c(df, spark):
    print("Ejercicio 2c:")
    df.write.mode("append").parquet("data/lake/bronze/sentimiento")
    df_sent = spark.read.parquet("data/lake/bronze/sentimiento")

    return df_sent
    
    