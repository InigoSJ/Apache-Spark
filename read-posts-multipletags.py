from pyspark import sql
spark = sql.SparkSession.builder.getOrCreate()
posts_df = spark.read.format('com.databricks.spark.xml').options(rowTag='row').load('gs://<your-bucket>/Posts.xml')

from pyspark.sql.functions import col,lit,concat
from pyspark.sql import functions as F
print("File loaded")
print('Col names and types ',posts_df.dtypes)

posts_df = posts_df.select(col("_AnswerCount").alias("AnswerCount"), col("_Id").alias("question_id"), col("_CreationDate").alias("creation_date"), col("_Title").alias("title"), col("_Tags").alias("tags"))

posts_df = posts_df.where((col("tags").isNotNull()) & (col("title").isNotNull()))
posts_df = posts_df.withColumn("is_answered", F.when(col("AnswerCount").isNull(), False).otherwise(True))
posts_df = posts_df.withColumn("link", concat(lit("https://stackoverflow.com/questions/"),col("question_id").cast("STRING"),lit("/"), col("title").cast("STRING")))
posts_df = posts_df.drop("AnswerCount")
posts_df = posts_df.withColumn("tags", F.translate(col("tags"),"<","")).withColumn("tags",F.split(col("tags"),">"))
posts_df.show()
print("First transformations made")

# TODO:We should get `tags_list` below from some file, where we are reading all the BQ tags
def contains_udf(row):
    tags_list=["google-cloud-dataflow", "google-cloud-composer", "google-bigquery", "google-cloud-dataproc"]
    contains = False
    for element in row:
            if element in tags_list:
                    contains=True
    return contains
udf_two = F.udf(contains_udf)
posts_df = posts_df.withColumn("containsvalue", udf_two(posts_df.tags)).where(col("containsvalue") == True).drop("containsvalue")
print("Udf transformation done")
posts_df = posts_df.withColumn("tags", F.concat_ws(',',col('tags')))
posts_df.show()
posts_df.write.json("gs://<your-bucket>/Posts_filtered_multipletags.json")