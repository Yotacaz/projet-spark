from pyspark.sql import SparkSession
from typing import cast
#
builder = cast(SparkSession.Builder, SparkSession.builder)

spark = builder.appName("monApp").master("local").getOrCreate()
sc = spark.sparkContext

def main():
    print("Hello from projet-spark!")


if __name__ == "__main__":

    main()
