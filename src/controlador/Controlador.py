from src.modelo.ejercicios.ejercicio1 import *
from src.modelo.conexion.SparkSession import *

def ejecutar():

    df, spark = crear_sesion()

    # Ejercicio 1b
    df_ejb = ejercicio1a(spark, "Sector", "01/10/2024", "01/10/2026")
    df_ejb.show(10)