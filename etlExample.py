"""The starting point of every Spark application is the creation of a SparkSession. 
This is a driver process that maintains all relevant information about your Spark Application and 
it is also responsible for distributing and scheduling your application across all executors. 
We can simply create a SparkSession in the following way"""

def loadDFWithoutSchema(spark):

    df = spark.read.format("csv").option("header", "true").load(environ["HOME"] + "/Downloads/autos.csv")

    return df
    
    
 """The getOrCreate method will try to get a SparkSession if one is already created, otherwise it will create a new one. 
 With the master option it is possible to specify the master URL that is being connected. 
 However, because we’re running our job locally, we will specify the local[*] argument. 
 This means that Spark will use as many worker threads as logical cores on your machine. 
 We set the application name with the appName option, this name will appear in the Spark UI and log data.
 Our next step is to read the CSV file. Reading in a CSV can be done with a DataFrameReader that is associated with our SparkSession. 
 In doing so, Spark allows us to specify whether schema inference is being used as well as some other options"""
 
 def loadDFWithoutSchema(spark):

    df = spark.read.format("csv").option("header", "true").load(path)

    return df
    
    """Whether to choose for schema inference or manually defining a schema depends heavily on the use case, 
    in case of writing an ETL job for a production environment, 
    it is strongly recommended to define a schema in order to prevent inaccurate data representation. 
    Another constraint of schema inference is that it tends to make your Spark application slower, 
    especially when working with CSV or JSON. 
    Therefore, I’m also showing how to read in data with a prior defined schema:"""

def loadDFWithSchema(spark):
    
schema = StructType([
        StructField("dateCrawled", TimestampType(), True),
        StructField("name", StringType(), True),
        StructField("seller", StringType(), True),
        StructField("offerType", StringType(), True),
        StructField("price", LongType(), True),
        StructField("abtest", StringType(), True),
        StructField("vehicleType", StringType(), True),
        StructField("yearOfRegistration", StringType(), True),
        StructField("gearbox", StringType(), True),
        StructField("powerPS", ShortType(), True),
        StructField("model", StringType(), True),
        StructField("kilometer", LongType(), True),
        StructField("monthOfRegistration", StringType(), True),
        StructField("fuelType", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("notRepairedDamage", StringType(), True),
        StructField("dateCreated", DateType(), True),
        StructField("nrOfPictures", ShortType(), True),
        StructField("postalCode", StringType(), True),
        StructField("lastSeen", TimestampType(), True)
    ])

    df = spark \
        .read \
        .format("csv") \
        .schema(schema)         \
        .option("header", "true") \
        .load(environ["HOME"] + "/Downloads/autos.csv")

    return df
    
    ### To see few records just hit and use take(5) to see first five records.
    
    df.take(5)
    
    
    ###there are multiple columns containing null values. We can handle missing data with a wide variety of options.
    However, discussing this is out of the scope of this article. As a result, we choose to leave the missing values as null. 
    However, there are more strange values and columns in this dataset, so some basic transformations are needed ###
    
    def clean_drop_data(df):

    df_dropped = df.drop("dateCrawled","nrOfPictures","lastSeen")
    df_filtered = df_dropped.where(col("seller") != "gewerblich")
    df_dropped_seller = df_filtered.drop("seller")
    df_filtered2 = df_dropped_seller.where(col("offerType") != "Gesuch")
    df_final = df_filtered2.drop("offerType")
    
    return df_final
    
    To Load Data in hive or any Local Db.
    
    We use below commands:
    
    df_final.write.saveAsTable("schemaName.tableName");
    
    or 
    
    df.select(df.col("col1"),df.col("col2"), df.col("col3")) .write().mode("overwrite").saveAsTable("schemaName.tableName");
    
    

    return df_final
    
    
    
    
