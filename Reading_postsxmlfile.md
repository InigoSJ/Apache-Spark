## READING POSTS.XML FILE

**NOTE: This tutorial assumes you are working in a Google Cloud Dataproc  Environment. If you are not, and you are running
pyspark locally i.e., then you may find some configuration issues..
For example: when loading files using `com.databricks.spark.xml` you will need to have that module installed 
(see Readme.md file in this same repo to see how to solve that problem).

TODO: add info about the files used

TODO: remove bucket names from the files. 

We will use the [GCS connector ](https://cloud.google.com/dataproc/docs/tutorials/gcs-connector-spark-tutorial), which is
automatically installed for new Dataproc clusters. This allows us to easily access files in GCS by using the `gs://` path, something like:

```python
>>tags = sc.textFile("gs://<your-bucket>/Tags.xml")
>>tags.count()
570
```

Enter your dataproc master and run:
```bash
pyspark
```
It will return something like:

```bash
>>Using Python version 2.7.13 (default, Sep 26 2018 18:42:22)
>>SparkSession available as 'spark'.
```
Now let's run our python code:

```python
df = spark.read.format('com.databricks.spark.xml').options(rowTag='row').load('gs://<your-bucket>/Tags.xml')
```

If we get error:

```
: java.lang.ClassNotFoundException: Failed to find data source: com.databricks.spark.xml. Please find packages at http://spark.apache.org/third-party-projects.h
```

As we can see in [this link](https://cloud.google.com/blog/products/data-analytics/managing-java-dependencies-apache-spark-applications-cloud-dataproc),
we should be able to use the missing dependency by either:
 <li> adding the `--packages=[DEPENDENCIES]` if directly submitting job with `spark-submit` command.</li>
        
    `spark-submit --packages='com.databricks.spark.xml'`
<li> using flag `--properties spark.jars.packages=[DEPENDENCIES] if sending the command using `gcloud command`</li>

Using code below (file `read-posts.py`):

```python
import pyspark

sc = pyspark.SparkContext()
sc.setLogLevel("DEBUG")
from pyspark import sql
spark = sql.SparkSession.builder.getOrCreate()
df = spark.read.format('com.databricks.spark.xml').options(rowTag='row').load('gs://<your-bucket>/Posts.xml')
rows = df.count()
print(rows)
``` 

Run it with command:

```bash
gcloud dataproc jobs submit pyspark --cluster <your-cluster-name> --region <your-region>--properties spark.jars.packages="com.databricks:spark-xml_2.11:0.5.0" read-posts.py 
```

You will get a JobId , you can check its logs. Be aware that in our code in `read-posts.py` we have set logging level
to `DEBUG` therefore there will be excessive logging. You can change this by modifying line:

`
sc.setLogLevel("DEBUG")
`

Working with `Tags.xml` simply for trying changes we'll perform:
------------------

```python
tags_df = spark.read.format('com.databricks.spark.xml').options(rowTag='row').load('Tags.xml')
tags_df.count()
>>567
tags_df.dtypes
>> [('_Count', 'bigint'), ('_ExcerptPostId', 'bigint'), ('_Id', 'bigint'), ('_TagName', 'string'), ('_WikiPostId', 'bigint')]

# Changing column names so that they don't contain underscores
# at the beginning of the word

from pyspark.sql.functions import col
tags_df = tags_df.select(col("_Count").alias("Count"),col("_ExcerptPostId").alias("ExcerptPostId"),col("_Id").alias("Id"),col("_TagName").alias("TagName"),col("_WikiPostId").alias("WikiPostId"))
tags_df.dtypes
>>[('Count', 'bigint'), ('ExcerptPostId', 'bigint'), ('Id', 'bigint'), ('TagName', 'string'), ('WikiPostId', 'bigint')]


# Changing column type. 
tags_df = tags_df.withColumn("Count", tags_df["Count"].cast("int"))

# If you don't know the short values like "int" you can always use:
from pyspark.sql.types import IntegerType
tags_df.withColumn("Count", tags_df["Count"].cast(IntegerType()))


```

CREATE A SMALLER `posts.xml` FILE TO WORK WITH
--------------------

Send the `split-posts-file.py` file to your Cluster to run a pyspark job by running the following command:

```bash
gcloud dataproc jobs submit pyspark --cluster cluster-pyspark-1 --region global --properties spark.jars.packages="com.databricks:spark-xml_2.11:0.5.0" split-posts-file.py
```

This job should:
<li> Read the `Posts.xml` file </li>
<li>Get a smaller file containing only the rows with _Id between 0 and 1000</li>
<li>Write out the file to: json, parquet and orc</li>


WORKING WITH THE SMALLER `posts.xml` FILE TO VERIFY WHICH OPERATIONS WE WANT TO RUN ON DATA
-------------------

```python

posts_df = spark.read.json("Posts.json")
posts_df.count()

>>> 397
```

We had created our file getting the rows with `Id` between 0-1000. However we are only getting 397 records. This is because
for the deleted posts there are no _Ids.

Get only the column names we are interested in, renaming them as we need:

```python
posts_df = posts_df.select(col("_AnswerCount").alias("AnswerCount"), col("_Id").alias("question_id"), col("_CreationDate").alias("creation_date"), col("_Title").alias("title"), col("_Tags").alias("tags")) 
```

Our table will look something like:

```python
+-----------+-----------+--------------------+--------------------+--------------------+
|AnswerCount|question_id|       creation_date|               title|                tags|
+-----------+-----------+--------------------+--------------------+--------------------+
|         13|          4|2008-07-31T21:42:...|Convert Decimal t...|<c#><floating-poi...|
|          6|          6|2008-07-31T22:08:...|Percentage width ...|<html><css><css3>...|
|       null|          7|2008-07-31T22:17:...|                null|                null|
|         63|          9|2008-07-31T23:40:...|How do I calculat...|<c#><.net><datetime>|
|         36|         11|2008-07-31T23:55:...|Calculate relativ...|<c#><datetime><ti...|
|       null|         12|2008-07-31T23:56:...|                null|                null|
+-----------+-----------+--------------------+--------------------+--------------------+
```

Those rows which have title and tags null should be because they are not actually posts. They are answers. Therefore getting
rid of them.

 TODO: here there is one thing, the following two return the same amount of values:
 
 ```python
posts_df.where((col("tags").isNotNull()) & (col("title").isNotNull())).count()
>> 118
posts_df.where((col("AnswerCount").isNotNull()) & (col("tags").isNotNull()) & (col("title").isNotNull())).count()
>> 118
```

That could indicate that either for the first 0<Id<1000 there are no posts without answers, or that there is no info about
unanswered posts?

Just in case we'll run it like:

```python
posts_df = posts_df.where((col("tags").isNotNull()) & (col("title").isNotNull()))
```
Create `is_answered` column (assuming we have those values), create `link` column and drop `AnswerCount`.

```python
posts_df = posts_df.withColumn("is_answered", F.when(col("AnswerCount").isNull(), False).otherwise(True))
posts_df = posts_df.withColumn("link", concat(lit("https://stackoverflow.com/questions/"),col("question_id").cast("STRING"),lit("/"), col("title").cast("STRING")))
posts_df = posts_df.drop("AnswerCount")
posts_df = posts_df.withColumn("tags", F.translate(col("tags"),"<","")).withColumn("tags",F.split(col("tags"),">"))
```

TODO: About last line: Our 'tags' rows will contain tags like: `<tag><tag>`. We replace all "<" with "" leaving the row like:
"tag>tag>". Now we split col by ">" leaving it like: [tag,tag ] I'm not sure what happens with the last tag. Does it remaine 
with a blank space? Therefore if we would have: "bigquery" tag, we would actually have "bigquery " generating potential issues
when matching tags?  

Filter values only by those tags we are interested in (we use now "c#" because we don't have google-cloud tags in this
json containing firs posts from SO). For this we'll use UDF functions:

```python
def contains_udf(row,value="c#"):
    contains = False
    for element in row:
            if element==value:
                    contains=True
    return contains

udf_two = F.udf(contains_udf)
posts_df = posts_df.withColumn("containsvalue", udf_two(posts_df.tags,lit("c#"))).where(col("containsvalue") == True).drop("containsvalue")
``` 

Our DF now looks like:

```python
+-----------+--------------------+--------------------+--------------------+-----------+--------------------+
|question_id|       creation_date|               title|                tags|is_answered|                link|
+-----------+--------------------+--------------------+--------------------+-----------+--------------------+
|          4|2008-07-31T21:42:...|Convert Decimal t...|[c#, floating-poi...|       true|https://stackover...|
|          9|2008-07-31T23:40:...|How do I calculat...|[c#, .net, dateti...|       true|https://stackover...|
|         11|2008-07-31T23:55:...|Calculate relativ...|[c#, datetime, ti...|       true|https://stackover...|
+-----------+--------------------+--------------------+--------------------+-----------+--------------------+
```

If trying to upload the previous to our BQ table it will throw errors because
our `tags` column is String, not array.

```python
posts_df = posts_df.withColumn("tags", F.concat_ws(',',col('tags')))
posts_df.write.json("Posts_filtered.json",mode='overwrite')
```


To run the previous code on the full Posts.xml file we can use:

```bash
gcloud dataproc jobs submit pyspark --cluster cluster-pyspark-1 --region global --properties spark.jars.packages="com.databricks:spark-xml_2.11:0.5.0" read-posts-onlyonetag.py
```

If we want to filter by several tags we can use the file `read-posts-multipletags.py`, modifying the `tags_list` defined
in the `contains_udf` function. Run it doing:

```bash
gcloud dataproc jobs submit pyspark --cluster cluster-pyspark-1 --region global --properties spark.jars.packages="com.databricks:spark-xml_2.11:0.5.0" read-posts-multipletags.py
```

