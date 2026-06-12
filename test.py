from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test")
    .getOrCreate()
)

print("Spark =", spark.version)

df = spark.createDataFrame(
    [(1, "a"), (2, "b")],
    ["id", "name"]
)

df.show()

spark.stop()