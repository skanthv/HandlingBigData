
# Import PySpark libraries
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

'''
WHEN I GOT ERROR THAT CANNOT PICKLE model.pkl, one of the reasons/solutions suggested was this :
This error occurs when you try to pickle a PySpark object that is not serializable, 
such as a SparkSession or SparkContext object. To solve this error, 
you can create the SparkSession object inside a function and pass it as an argument 
to any other functions that need to use it. 
'''
def create_spark_session():
    # Create a Spark session
    spark = SparkSession.builder.appName("Linear Regression").getOrCreate()
    return spark

# Create a Spark session
spark = create_spark_session()


# Read the CSV file into a DataFrame
df = spark.read.format("csv").option("header", "true").load("file.csv")

# Convert the "date of birth" column to a date data type
df = df.withColumn("date of birth", col("date of birth").cast("date"))

# Convert the "height" column to a float data type
df = df.withColumn("height", col("height").cast("float"))
df = df.withColumn("ID", col("ID").cast("integer"))
df = df.withColumn("age", col("age").cast("integer"))

# Create a vector assembler for the features
assembler = VectorAssembler(inputCols=["ID", "age"], outputCol="features")
df = assembler.transform(df)

# Split the data into training and testing sets
training, testing = df.randomSplit([0.7, 0.3], seed=12345)

# Create a linear regression model
lr = LinearRegression(featuresCol="features", labelCol="height")

# Train the model on the training set
model = lr.fit(training)


'''
WHEN I TRIED TO SAVE THE MODEL TO A FILE WITH PICKLE LIBRARY, IT GAVE this error
TypeError: cannot pickle '_thread.RLock' object


import pickle
# Save the trained model as a pickle file
with open("model.pkl", "wb") as f:
    pickle.dump(model, f)

REASON
This error is probably occurring because you are trying to pickle an object that is not serializable, 
which is a common issue with the pyspark.ml models. 
Instead of using the pickle module to save the model, 
you can use the built-in save method of the LinearRegressionModel object to save the model in the format that is compatible with pyspark.ml.

So used modified code that uses the save method instead of pickle:
'''


'''
when we save a file to spark, as it is a distributed file system , it creates a folder with that file name and stores the file contents in small parts in it
when the code os.remove tries to remove it, it realizes it is a directory and not a file , so gives error
IsADirectoryError: [Errno 21] Is a directory: 'model.pkl'

'''

import shutil
import os

# Check if the model file exists and remove it
if os.path.exists("model.pkl"):
    shutil.rmtree("model.pkl")
#if it was just a file, then you can remove it with this line
#    os.remove("model.pkl")


# Save the trained model as a pickle file
model.save("model.pkl")
