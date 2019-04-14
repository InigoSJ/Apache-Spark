### USING SPARK TO LOAD XML DATA COMING FROM STACKOVERFLOW

We will work on local without entering in any infrastructure-workers-slots setup. This is for simplicity, to focus only on the Python API for Spark.
However key about Spark is about setting up some amount of workers...

Install pyspark library:

```python
virtualenv -p python3 python3_sparkenv
pip install pyspark
```

Spark uses Java under the hood; If you get some Java related errors when first working with pyspark library then more likely you don't have
java properly installed in your machine or maybe you don't have $JAVA_HOME env_variable properly defined.

Creating a Spark Context, not sure what this is for. 

```python
from pyspark import SparkContext
sc = SparkContext("local","myfirstapp")
```

Setting logging to INFO (instead of the official: WARNING)

```python
sc.setLogLevel("INFO")
```

Loading xml file. Loading the file this way will use the higher level of abstraction from Spark: RDD, resilient distributed dataset.
```python
tags_xml = sc.textFile("Tags.xml")
```

You can run things like; 

```python
tags_xml.count() 
tags_xml.take(10)

```

However I believe the previous will just get the xml as raw file, reading lines one by one, with all the contents...

However it should be more recommedend to use the more intutitive pyspark.sql module to create Dataframes out of data.

```python
from pyspark import sql 
spark = sql.SparkSession.builder.getOrCreate()
df = spark.read.format('com.databricks.spark.xml').options(rowTag='tags').load('<your-dir>Tags.xml')
```

If the previous return some error like: 

```java
py4j.protocol.Py4JJavaError: An error occurred while calling o378.load.
: java.lang.ClassNotFoundException: Failed to find data source: com.databricks.spark.xml. Please find packages at http://spark.apache.org/third-party-projects.html
```
Then it should mean that you don't have the "spark-xml_2.11-0.5.0.jar" file installed. 
This should be in /your-python-env/lib/python3/site-packages/pyspark/jars. If it is not there, downloading from the mvn repo (https://mvnrepository.com/artifact/com.databricks/spark-xml_2.11/0.5.0)

```bash
wget http://central.maven.org/maven2/com/databricks/spark-xml_2.11/0.5.0/spark-xml_2.11-0.5.0.jar
```

Continuing with our loaded file. It is already converted to a DataFrame:

```python
>>> type(df)
<class 'pyspark.sql.dataframe.DataFrame'>
```

Now since our `Tags.xml` file looks like:
```xml
<?xml version="1.0" encoding="utf-8"?>
<tags>
  <row Id="1" TagName="bug" Count="5696" ExcerptPostId="250212" WikiPostId="250211" />
  <row Id="2" TagName="feature-request" Count="6010" ExcerptPostId="250232" WikiPostId="250231" />
```

if we would run
```python
>>> df.count()
1
```
it  returns 1. This is because there is only one element in the tag: `<tags>`. Instead we should load the file using the `<row>`  tag:

```python
>>> df = spark.read.format('com.databricks.spark.xml').options(rowTag='row').load('<your-dir>Tags.xml')
>>> df.count()
567
```

Now we can perform more operations on it:
```python
>>> df.columns
['_Count', '_ExcerptPostId', '_Id', '_TagName', '_WikiPostId']

>>> df.filter(df._Id<3).take(1)
[Row(_Count=5696, _ExcerptPostId=250212, _Id=1, _TagName='bug', _WikiPostId=250211)]
```
