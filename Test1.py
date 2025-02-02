import pyspark
import pandas as pd

df = pd.read_csv('test.csv')
# print(df)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dataframe').getOrCreate()

# df_pyspark = spark.read.option('header', 'true').csv('test.csv', inferSchema = True)

df_pyspark = spark.read.csv('test.csv', header=True, inferSchema=True)
df_pyspark.show()
df_pyspark.printSchema()




