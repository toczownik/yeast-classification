import sys

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructField, StructType, StringType, DecimalType


if __name__ == '__main__':
    server = sys.argv[1] if len(sys.argv) == 2 else "localhost:9092"

    spark = SparkSession.builder.appName("yeast-classification").getOrCreate()
    random_forest = PipelineModel.load("yeast_forest/")
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("subscribe", "yeast")
        .load()
    )

    json_schema = StructType([
        StructField("name", StringType(), True),
        StructField("mcg", DecimalType(3, 2), True),
        StructField("gvh", DecimalType(3, 2), True),
        StructField("alm", DecimalType(3, 2), True),
        StructField("mit", DecimalType(3, 2), True),
        StructField("erl", DecimalType(3, 2), True),
        StructField("pox", DecimalType(3, 2), True),
        StructField("vac", DecimalType(3, 2), True),
        StructField("nuc", DecimalType(3, 2), True)
    ])

    parsed = raw.select(
        "value", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("json").getField("name").alias("name"),
        f.col("json").getField("mcg").alias("mcg"),
        f.col("json").getField("gvh").alias("gvh"),
        f.col("json").getField("alm").alias("alm"),
        f.col("json").getField("mit").alias("mit"),
        f.col("json").getField("erl").alias("erl"),
        f.col("json").getField("pox").alias("pox"),
        f.col("json").getField("vac").alias("vac"),
        f.col("json").getField("nuc").alias("nuc")
    )

    query = (
        random_forest.transform(parsed).writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination(60)
    query.stop()
