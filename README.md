# Spark
In this article, I’m going to demonstrate how Apache Spark can be utilised for writing powerful ETL jobs in Python. If you’re already familiar with Python and working with data from day to day, then PySpark is going to help you to create more scalable processing and analysis of (big) data.

What is Apache Spark?
Apache Spark is one of the most popular engines for large-scale data processing. It’s an open source system with an API supporting multiple programming languages. Processing of data is done in memory, hence it’s several times faster than for example MapReduce. Spark comes with libraries supporting a wide range of tasks, such as streaming, machine learning and SQL. It’s able to run from your local computer, but also can be scaled up to a cluster of hundreds of servers.

What is ETL?
ETL (Extract, Transform and Load) is the procedure of migrating data from one system to another. Data extraction is the process of retrieving data out of homogeneous or heterogeneous sources for further data processing and data storage. During data processing, the data is being cleaned and incorrect or inaccurate records are being modified or deleted. Finally, the processed data is loaded (e.g. stored) into a target database such as a data warehouse or data lake.

Extract
The starting point of every Spark application is the creation of a SparkSession. This is a driver process that maintains all relevant information about your Spark Application and it is also responsible for distributing and scheduling your application across all executors. We can simply create a SparkSession in the following way:

def initialize_Spark():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("simple etl job") \
        .getOrCreate()

    return spark
    
The getOrCreate method will try to get a SparkSession if one is already created, otherwise it will create a new one. With the master option it is possible to specify the master URL that is being connected. However, because we’re running our job locally, we will specify the local[*] argument. This means that Spark will use as many worker threads as logical cores on your machine. We set the application name with the appName option, this name will appear in the Spark UI and log data.

Our next step is to read the CSV file. Reading in a CSV can be done with a DataFrameReader that is associated with our SparkSession. In doing so, Spark allows us to specify whether schema inference is being used as well as some other options.

Whether to choose for schema inference or manually defining a schema depends heavily on the use case, in case of writing an ETL job for a production environment, it is strongly recommended to define a schema in order to prevent inaccurate data representation. Another constraint of schema inference is that it tends to make your Spark application slower, especially when working with CSV or JSON. Therefore, I’m also showing how to read in data with a prior defined schema.
