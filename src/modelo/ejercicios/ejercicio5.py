from delta.tables import DeltaTable
from pyspark.sql.functions import *

def ejercicio5a(spark):
    print("Ejercicio 5a: ")
    df = spark.read.format("delta").load("data/lake/bronze/ibex")

    df_nuevo = df.withColumn("Close_mas5", col("Close") + 5)

    # Hacemos un try para no parar la ejecución
    try:
        df_nuevo.write.format("delta").mode("append").save("data/lake/bronze/ibex")
    except Exception as e:
        print(e)

def ejercicio5b(spark):
    print("Ejercicio 5b: ")
    df = spark.read.format("delta").load("data/lake/bronze/ibex")

    df_nuevo = df.withColumn("Close_mas5", col("Close") + 5)
    df_nuevo.write.format("delta").mode("append").option("mergeSchema", "true").save("data/lake/bronze/ibex")

def ejercicio5c(spark):
    print("Ejercicio 5c: ")

    destino = DeltaTable.forPath(spark, "data/lake/bronze/ibex")

    print("Filas antes de borrar BBVA: ", destino.toDF().count())
    destino.delete("Ticker='BBVA.MC'")
    print("Filas despues de borrar BBVA: ", destino.toDF().count())

def ejercicio5d(spark):
    print("Ejercicio 5d:")

    DeltaTable.forPath(spark, "data/lake/bronze/ibex").history().show()

    df = spark.read.format("delta").option("versionAsOf",1).load("data/lake/bronze/ibex")
    df.show(5)
    df.printSchema()
    