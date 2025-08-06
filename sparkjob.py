from pyspark.sql import SparkSession
spark = SparkSession.builder \
                .appName('AirBnB Spark Job') \
                .getOrCreate()  



df= spark.read.csv("AB_NYC_2019.csv", header=True, inferSchema=True, sep=',')

#remove null value entries
rem_df = df.dropna(how='all', subset=['name', 'neighbourhood_group', 'neighbourhood', 'latitude', 'longitude', 'room_type', 'price'])


#filter places where price is greater than 100
filtered_df = rem_df.filter(rem_df.price > 100)

#new column
new_df = filtered_df.withColumn("Estimate_Price_Values", filtered_df.price * filtered_df.availability_365)


#top Earning airbnbs

top_earners = new_df.select("id", "name", "neighbourhood", "Estimate_Price_Values", "price", "availability_365").orderBy("Estimate_Price_Values", descending=True)


#show the top earners
top_earners.show(n=10, truncate=False)


# Save the top earners to a CSV file
#top_earners.write.mode("overwrite").csv("output/top_earners.csv", header=True)
#top_earners.write.csv("output/room_summary.csv", header=True, mode="overwrite")

#df.write.mode("overwrite").option("header", True).csv("output/output_csv")
#df.coalesce(1).write.mode("overwrite").option("header", True).csv("output_csv")
df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/temp_room_summary")

#stop the spark session
spark.stop()