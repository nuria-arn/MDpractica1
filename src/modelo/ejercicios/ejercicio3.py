from delta.tables import DeltaTable

def ejercicio3(spark):
    print("Ejercicio 3:")
    DeltaTable.convertToDelta(spark, "parquet.`data/lake/bronze/ibex`")
    DeltaTable.forPath(spark, "data/lake/bronze/ibex").history().show()

    DeltaTable.convertToDelta(spark, "parquet.`data/lake/bronze/sentimiento`")
    DeltaTable.forPath(spark, "data/lake/bronze/sentimiento").history().show()