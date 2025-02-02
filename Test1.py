import pyspark
import pandas as pd

df = pd.read_csv('test.csv')
# print(df)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dataframe').getOrCreate()

# Type 1 of reading data
# df_pyspark = spark.read.option('header', 'true').csv('test.csv', inferSchema = True)

# Type 2 of reading data
df_pyspark = spark.read.csv('test.csv', header=True, inferSchema=True)

# df_pyspark.show()
# df_pyspark.printSchema()

# df_pyspark.describe().show()

# Adding a New column in Dataframe

# df_pyspark.withColumn('Age after 5 Yrs', df_pyspark['age']+5).show()

# Drop an existing column

# df_pyspark.drop('Age after 5 Yrs').show()

# Renaming an existing column

# df_pyspark.withColumnRenamed('Name', 'Full Name').show()

# Drop Rows where Null is present

df_pyspark.na.drop().show()

# Drop Rows on How

df_pyspark.na.drop(how="all").show()

# Drop Rows on subset

df_pyspark.na.drop(how="all", subset=['Name']).show()

