# Ayuda de ChatGPT-6 para el bucle donde cargamos los nuevos datos en el df

from delta.tables import DeltaTable
from datetime import date
from src.modelo.ejercicios.ejercicio1 import *

def ejercicio4(spark, empresas):
    print("Ejercicio 4: ")
    destino = DeltaTable.forPath(spark, "data/lake/bronze/ibex")
    
    # Primero consultamos la versión anterior de la tabla
    destino.history().show()
    df_actual = spark.read.format("delta").load("data/lake/bronze/ibex")
    filas_antes = df_actual.count()

    # Descargamos los datos nuevos
    df_nuevos = None
    for empresa in empresas:
        df_empresa = ejercicio1a( spark, empresa, "2026-10-01", date.today().isoformat())

        if df_nuevos is None:
            df_nuevos = df_empresa
        else:
            df_nuevos = df_nuevos.union(df_empresa)
    
    (destino.alias("d")
        .merge(df_nuevos.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date")
        .whenNotMatchedInsertAll()
        .execute())

    filas_despues = spark.read.format("delta").load("data/lake/bronze/ibex").count()

    print(f"Filas antes: {filas_antes}")
    print(f"Filas despues: {filas_despues}")
    print(f"Filas insertadas: {(filas_despues - filas_antes)}")