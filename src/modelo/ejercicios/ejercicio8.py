from pyspark.sql.functions import *

def ejercicio8(spark):
    df_bronze = spark.read.format("delta").load("data/lake/bronze/sentimiento")

    print('Filas Bronze: ', df_bronze.count())

    # Cconvertimos fecha a date
    df = df_bronze.withColumn("Fecha", to_date(col("Fecha"), "yyyy-MM-dd"))

    # Eliminar nulos
    df = df.dropna()

    # Eliminar duplicados
    df = df.dropDuplicates()

    # Unificar . y ,
    df = df.withColumn("Sentimiento", regexp_replace(col("Sentimiento"), ",", "."))

    # Convertir Sentimiento a double
    df = df.withColumn("Sentimiento", col("Sentimiento").cast("double"))


    df.write.format("delta").mode("overwrite").save("data/lake/silver/sentimiento")
    print("Filas silver: ", df.count())

    df.printSchema()
    df.show(10)