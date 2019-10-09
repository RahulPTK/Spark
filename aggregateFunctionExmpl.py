we will look at the Aggregation function in Spark, that allows us to apply different aggregations on columns in Spark

###The agg() function in PySpark###

###Basically, there is one function that takes care of all the agglomerations in Spark. 
This is called “agg()” and takes some other function in it. There are various possibilities, the most common ones are building sums, 
calculating the average or max/min values. 
The “agg()” function is called on a grouped dataset and is executed on one column. 
In the following samples, some possibilities are shown"""

from pyspark.sql.functions import *
df_ordered.groupby().agg(max(df_ordered.price)).collect()

###In this example, we imported all available functions from pyspark.sql. 
We called the “agg” function on the df_ordered dataset.
We than use the “max()” function that retrieves the highest value of the price"""

For, avg(), sum(), mean(), count()

df_ordered.groupby().agg(avg(df_ordered.price)).collect()

df_ordered.groupby().agg(sum(df_ordered.price)).collect()

df_ordered.groupby().agg(mean(df_ordered.price)).collect()

df_ordered.groupby().agg(count(df_ordered.price)).collect()


For order data,

###We will continue with the sort statement in Spark that allows us to sort data in Spark and 
then we will focus on the groupby statement in Spark,
which will eventually allow us to group data in Spark###

Order Data in Spark

First, let’s look at how to order data. Basically, there is an easy function on all dataframes, called “orderBy”. 
This function takes several parameters, but the most important parameters for us are:

Columns: a list of columns to order the dataset by. This is either one or more items
Order: ascending (=True) or descending (ascending=False)

If we again load our dataset from the previous sample, we can now easily apply the orderBy() function. 
In our sample, we order everything by personid:

df_new = spark.read.parquet(fileName)
df_ordered = df_new.orderBy(df_new.personid, ascending=True)
df_ordered.show()

Filter Data in Spark

Next, we want to filter our data. We take the ordered data again and only take customers that are between 33 and 35 years.
Just like the previous “orderby” function, we can also apply an easy function here: filter. 
This function basically takes one filter argument. In order to have a “between”, we need to chain two filter statements together.
Also like the previous sample, the column to filter with needs to be applied:

df_filtered = df_ordered.filter(df_ordered.age < 35).filter(df_ordered.age > 33)
df_filtered.show()

Group Data in Spark
In order to make more complex queries, we can also group data by different columns. 
This is done with the “groupBy” statement. 
Basically, this statement also takes just one argument – the column(s) to sort by. 
The following sample is a bit more complex, but I will explain it after the sample:

from pyspark.sql.functions import bround
df_grouped = df_ordered \
    .groupBy(df_ordered.personid) \
    .sum("price") \
    .orderBy("sum(price)") \
    .select("personid", bround("sum(price)", 2)) \
    .withColumnRenamed("bround(sum(price), 2)", "value")
df_grouped.show()

So, what has happened here? We had several steps:

Take the previously ordered dataset and group it by personid
Create the sum of each person’s items
Order everything descending by the column for the sum. NOTE: the column is named “sum(price)” since it is a new column
We round the column “sum(price)” by two decimal points so that it looks nicer. 
Note again, that the name of the column is changed again to “ground(sum(price), 2)”
Since the column is now at a really hard to interpret name, 
we call the “withColumnRenamed” function to give the column a much nicer name.


We call our column “value”


