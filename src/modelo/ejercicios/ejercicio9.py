from pyspark.sql.functions import *

def ejercicio9(spark):
    print("Ejercicio 9: ")
    df = spark.read.format("delta").load("data/lake/bronze/ibex")

    print("Filas Bronze:", df.count())

    # Eliminar nulos
    df = df.dropna()

    # Eliminar duplicados
    df = df.dropDuplicates()

    df.write.format("delta").mode("overwrite").save("data/lake/silver/ibex")

    print("Filas Silver:", df.count())

    df.printSchema()
    df.show(5, truncate=False)
