from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").config("spark.driver.memory", "512m").appName("test").getOrCreate()
print("OK")
spark.stop()
