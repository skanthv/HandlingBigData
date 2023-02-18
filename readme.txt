'''
if we have a big data file..of say 100GB..then how will you handle it..
handling it refers to .. reading it, selecting records in it, filtering based on certain condition, training a machin learning model on it
running model on test data..etc..
for this you use Spark Distributed computing framework... which is one of the big data technologies or ecosystems


Spark is alternative to hadoop.. it is open source distributed computing ecosystem
the keydiff is hadoop does reads/writes to harddisk while doing map reduce.. but spark does read/writes to RAM and only once it does write to harddisk..so faster

BOth spark and hadoop run on  distributed file system called  hdfs .i.e. the file system handles large files by splitting them and storing them in parts on number of nodes and ensuring tehy are available in case of failure of nodes by replicating them..

in spark using the IDE you can write programs in languages such as python ,scala etc

in spark IDE to write python code, you use PySpark Library

so importing pyspark, you can then write python code and access distributed datafiles on hdfs filesystem
do the analysis using machine learning code, and create models

key point to note is that when you are reading files, they can be 100's of GB files stored across 100's of computers on teh spark cluster being managed by hdfs file ssytem..
it is still able to get the file contents, get column, row, do row or col manipulations etc..using pyspark library ..
on spark distributed computing framework

'''
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practice').getOrCreate()

'''
#with pandas you would read like this
import pandas as pd
df=pd.read_csv("data1.csv")
print(df)
print(type(df))
'''

df_pyspark=spark.read.csv('data1.csv')
print(df_pyspark)

#spark.read.option('header','true').csv('data1.csv').show()
df_pyspark=spark.read.csv('data1.csv',header=True,inferSchema=True)
df_pyspark.show()
print(type(df_pyspark))
print(df_pyspark.printSchema())

#show col
df_pyspark.select('name').show()
df_pyspark.select(['name','age']).show()
print(df_pyspark.dtypes)
df_pyspark.describe().show()


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
