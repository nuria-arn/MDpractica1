# uso de la IA Claude para cambiar los nombres de las columnas que dan error al convertir a delta
import yfinance as yf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *


# Crear funcion que permita descargar historico entre dos fechas
# Almacenarlo en un dataframe de pyspark

def ejercicio1a(spark, accion, fecha_ini, fecha_fin):
    pdf = yf.Ticker(accion).history(start = fecha_ini, end = fecha_fin, 
                                    interval="1d", auto_adjust = False)

    pdf.reset_index(inplace=True)

    pdf["Ticker"] = accion
    pdf.columns = [c.replace(" ", "_") for c in pdf.columns]

    df = spark.createDataFrame(pdf)

    return df

def ejercicio1c(spark):
    df = spark.read.parquet("data/lake/bronze/ibex")
    
    df.printSchema()
    df.show(5)

    df.select(["Ticker"]).show(40)
    df.select(["Date"]).show()
    df.select(["Close"]).show()

    return df
