import yfinance as yf
from pyspark.sql.functions import *

def ejercicio_5a(spark):
    df = spark.read.format("delta").load("data/lake/bronze/ibex")

    df_nuevo = df.withColumn("Close_mas5", col("Close") + 5)
    df_nuevo.write.format("delta").mode("append").save("data/lake/bronze/ibex")