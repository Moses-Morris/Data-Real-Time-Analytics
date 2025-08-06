from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, lower, when

spark = SparkSession.builder \
                .appName('AirBnB Data Processing') \
                .getOrCreate()

# Load the Airbnb dataset
df = spark.read.csv("AB_NYC_2019.csv", header=True, inferSchema=True)


df_cleaned = df \
    .dropna(how='all', subset=['name', 'neighbourhood_group', 'neighbourhood', 'latitude', 'longitude', 'room_type', 'price']) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("neighbourhood_group", trim(col("neighbourhood_group"))) \
    .withColumn("neighbourhood", trim(col("neighbourhood"))) \
          




filtered_df = df_cleaned.filter(col("price") > 100)

manhattan_df = df_cleaned \
    .filter( (col("price") > 0))

# Add a new column for estimated revenue
manhattan_df = manhattan_df.withColumn(
    "estimated_revenue",
    col("price") * col("availability_365")
)
top_listings = manhattan_df.select(
    "name", "room_type", "price", "availability_365", "estimated_revenue"
).orderBy(col("estimated_revenue").desc())

# Show top results
top_listings.show(10, truncate=False)

top_listings.write.mode("overwrite").csv("output/top_manhattan_listings", header=True)
# This print will NEVER execute until .show() or .collect() is called
df.filter(lambda row: print(row)).select("name")

# Stop Spark session
spark.stop()