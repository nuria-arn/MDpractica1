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
    df = spark.createDataFrame(pdf)

    return df

