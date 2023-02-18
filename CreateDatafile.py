#when a csv file is created by spark, it created an 8 part file..because it is thinking it is working in distributed file system..
# Import PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, when,col

# Create a Spark session
spark = SparkSession.builder.appName("CSV Creator").getOrCreate()

# Create a DataFrame with 20 rows
df = spark.range(1, 21).withColumnRenamed("id", "ID")
df = df.withColumn("name", concat(lit("Name"), col("ID")))
df = df.withColumn("sex", when(col("ID") % 2 == 0, "F").otherwise("M"))
df = df.withColumn("age", col("ID") + 20)
df = df.withColumn("date of birth", concat(lit("01/"), col("ID"), lit("/2000")))
df = df.withColumn("height", col("ID") + 150)

# Write the DataFrame to a CSV file
df.write.csv("file.csv", header=True)
