import pyspark
import pandas as pd

df = pd.read_csv('test.csv')
# print(df)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practice').getOrCreate()

print(spark.version)
spark.stop()