# Import PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum

# Create a Spark session
spark = SparkSession.builder.appName("CSV Reader").getOrCreate()

# Read CSV file into a DataFrame
df = spark.read.csv("file.csv", header=True, inferSchema=True)

# Show the DataFrame
df.show()

# Show column names and data types
df.printSchema()

# Show summary statistics of each column
df.describe().show()

# Add a row
new_row = spark.createDataFrame([(11, 'Jane', 'F', 25, '07/01/1997', 168)], df.schema)
df = df.union(new_row)

# Delete a row
df = df.filter(col("ID") != 3)

# Select rows that match a condition
df.filter((col("date of birth") > "01/01/2000") & (col("name").startswith("J"))).show()

# Add a column
df = df.withColumn("BMI", col("height")/(col("age")**2))

# Delete a column
df = df.drop("BMI")

# Show name, age columns of rows that match a condition
df.filter((col("date of birth") > "01/01/2000") & (col("name").startswith("J"))).select("name", "age").show()

# Group by sex and compute average age
df.groupBy("sex").agg(avg("age")).show()

# Compute total age of all rows
df.select(sum("age")).show()
