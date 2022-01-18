from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, lag
import os

parsedData = '/opt/airflow/sparkFiles/parsedData.csv'

spark = SparkSession \
    .builder \
    .appName("Pysparkexample") \
    .getOrCreate()

df = spark.read.csv(parsedData,
                    header='true',
                    inferSchema='true',
                    ignoreLeadingWhiteSpace=True,
                    ignoreTrailingWhiteSpace=True)

colDiffs = []
# except field named 'dataFor'
countyCols = df.columns[1:]
df = df.withColumn("dateFor", F.to_date("dateFor", "yyyy-MM-dd"))
windowSpec = Window.partitionBy().orderBy(F.col('dateFor').desc())
for county in countyCols:
    df = df.withColumn(f'{county}Diff', lead(county, 1).over(windowSpec))
    colDiffs.append(F.when((df[county] - df[f'{county}Diff']) < 0, 0)
                    .otherwise(df[county] - df[f'{county}Diff']).alias(county))
result = df.select('dateFor', *colDiffs).fillna(0)
result.toPandas().to_csv('/opt/airflow/sparkFiles/results.csv',
                         sep=',',
                         header=True,
                         index=False)

# delete the parsed data csv from the working directory
os.remove(parsedData)
