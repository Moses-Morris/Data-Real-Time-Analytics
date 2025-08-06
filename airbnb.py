from pyspark.sql import SparkSession

spark = SparkSession.builder \
                .appName('AirBnB') \
                .getOrCreate()


df = spark.range(5)
data = [("Tom", 30), ("Eva", 40), ("Pierre", 50)]
columns = ["Name", "Age"]

df.show()
df_2 = spark.createDataFrame(data, columns)

df_2.show()