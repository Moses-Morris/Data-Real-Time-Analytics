
from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder.appName("AirbnbAnalysis").getOrCreate()

# Load CSV
df = spark.read.csv("AB_NYC_2019.csv", header=True, inferSchema=True)

# Show sample
df.show(5)

df.printSchema()

#describe data
df.describe("name").show()

#count
count = df.count()
print(f"Total number of records: {count}")


#show distinct values in a column
distinct_neighbourhoods = df.select("neighbourhood").distinct()
distinct_neighbourhoods.show()


#filter data
filtered_df = df.filter(
    (df.price > 1000) &
    (df.neighbourhood.isNotNull()) &
    (df.neighbourhood == "Corona")
)
print(f"Filtered records count: {filtered_df} and Also unique to corona {filtered_df.show()}")


#select specific data
selected_df = df.select("name", "neighbourhood", "price").orderBy("price", ascending=False).show(10)
print(f"Top 10 most expensive listings: {selected_df}")


#average price
average_price = df.groupBy("neighbourhood").avg("price").orderBy("avg(price)", ascending=False).show(10)
print(f"Average price per neighbourhood: {average_price}")