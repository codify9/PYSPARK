import pyspark
import pandas as pd


df = pd.read_csv('test.csv')

# df = pd.read_csv(r"E:\PySpark\test.csv")
# print(df)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dataframe').getOrCreate()

# # Type 1 of reading data
# # df_pyspark = spark.read.option('header', 'true').csv('test.csv', inferSchema = True)

# # Type 2 of reading data
df_pyspark = spark.read.csv('test.csv', header=True, inferSchema=True)

df_pyspark.show()
df_pyspark.printSchema()

# # df_pyspark.describe().show()

# # Adding a New column in Dataframe

# # df_pyspark.withColumn('Age after 5 Yrs', df_pyspark['age']+5).show()

# # Drop an existing column

# # df_pyspark.drop('Age after 5 Yrs').show()

# # Renaming an existing column

# # df_pyspark.withColumnRenamed('Name', 'Full Name').show()

# # Drop Rows where Null is present

# # df_pyspark.na.drop().show()

# # Drop Rows on How

# # df_pyspark.na.drop(how="all").show()

# # Drop Rows on subset

# # df_pyspark.na.drop(how="all", subset=['Name']).show()

# # Filling the Null Values.

# # df_pyspark.na.fill('Na', ['Name', 'age']).show()

# # Filling the Null Values by Imputer Function

# # from pyspark.ml.feature import Imputer

# # imputer = Imputer(
# #     inputCols=['age', 'Experience', 'Salary'],
# #     outputCols=["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]  
# # ).setStrategy('mean')

# # imputer.fit(df_pyspark).transform(df_pyspark).show()

# # Filtering Data

# # print name of employees whose experience is less then 14.

# df_pyspark.filter('Experience <= 14').select(['Name', 'age']).show()

# df_pyspark3 = spark.read.csv('test3.csv', header=True, inferSchema=True)

# # df_pyspark3.show()

