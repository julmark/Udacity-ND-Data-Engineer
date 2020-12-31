import os
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, year, month, dayofmonth, weekofyear, dayofweek, date_format, to_date, initcap
from pyspark.sql.types import IntegerType, DoubleType



# The AWS key id and password are configured in a configuration file "config.cfg"
config = configparser.ConfigParser()
config.read('config.cfg')

# Reads the AWS access key information and saves them in a environment variable
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
output_data = config['DATALAKE']['OUTPUT_DATA']



def create_spark_session():
    """
    This function creates a session with Spark, the entry point to programming Spark with the Dataset and DataFrame API.
    """
    spark = SparkSession.builder.config("spark.jars.packages",
                                        "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")\
                .enableHiveSupport().getOrCreate()
    return spark



def etl_immigration(spark, input_path="immigration_data_sample.csv", output_path=None, date_output_path=None,
                         input_format = "csv", columns = ['cicid','i94res','i94cit','arrdate','i94addr','i94yr','i94mon','i94port','i94mode','i94visa','visatype',
                                                          'depdate','dtaddto','entdepa','entdepd','i94bir','biryear','gender','airline'],
                                                          load_size = None, partitionBy = ["i94yr", "i94mon"], **options):    
    """
    Reads the immigration dataset from the input_path, transforms it to the immigration fact table and dimension date table and saves both to the output_path
    output_path.
    
    Args:
        spark (SparkSession): Spark session
        input_path (string): Directory with the input files, default a data sample
        output_path (string): Directory to save immigration output files
        date_output_path (string): Directory to save date output files
        input_format (str): Format of the input files, default to "csv"
        columns (list): List of the columns names to read in
        load_size (int): Number of rows to read for debug purposes
        partitionBy (list): Files will be saved in partitions using the columns of this list
        options: All other string options
    """
    
    # Loads the immigration dataframe using Spark
    # After the data analysis we choose only selected columns straightaway
    dfImmigration = spark.read.format(input_format).load(input_path).select(columns)
    
    # Converts columns to integer
    dfImmigration = dfImmigration.withColumn("cicid", dfImmigration["cicid"].cast(IntegerType()))\
        .withColumn("i94res", dfImmigration["i94res"].cast(IntegerType()))\
        .withColumn("i94cit", dfImmigration["i94cit"].cast(IntegerType()))\
        .withColumn("arrdate", dfImmigration["arrdate"].cast(IntegerType()))\
        .withColumn("i94yr", dfImmigration["i94yr"].cast(IntegerType()))\
        .withColumn("i94mon", dfImmigration["i94mon"].cast(IntegerType()))\
        .withColumn("i94mode", dfImmigration["i94mode"].cast(IntegerType()))\
        .withColumn("i94visa", dfImmigration["i94visa"].cast(IntegerType()))\
        .withColumn("i94bir", dfImmigration["i94bir"].cast(IntegerType()))\
        .withColumn("biryear", dfImmigration["biryear"].cast(IntegerType()))
    
    # Converts SAS date to a string date in the format of YYYY-MM-DD with a udf
    dfImmigration = dfImmigration.withColumn("arrdate", convert_sas_udf(dfImmigration['arrdate']))\
        .withColumn("depdate", convert_sas_udf(dfImmigration['depdate']))
        
    # Creates a new column with the length of the visitor's stay in the US using udf
    dfImmigration = dfImmigration.withColumn('stay', date_diff_udf(dfImmigration.arrdate, dfImmigration.depdate))
    dfImmigration = dfImmigration.withColumn("stay", dfImmigration["stay"].cast(IntegerType()))\
    
    # Generates date dataframe and saves it to the date_output_path
    if date_output_path is not None:
        arrdate = dfImmigration.select('arrdate').distinct()
        depdate = dfImmigration.select('depdate').distinct()
        dates = arrdate.union(depdate)
        dates = dates.withColumn("date", to_date(dates.arrdate))
        dates = dates.withColumn("year", year(dates.date))
        dates = dates.withColumn("month", month(dates.date))
        dates = dates.withColumn("day", dayofmonth(dates.date))
        dates = dates.withColumn("weekofyear", weekofyear(dates.date))
        dates = dates.withColumn("dayofweek", dayofweek(dates.date))
        dates = dates.drop("date").withColumnRenamed('arrdate', 'date')
        dates.select(["date", "year", "month","day","weekofyear","dayofweek"]).write.save(date_output_path,mode= "overwrite",format="parquet")
    
    # Saves the immigration dataframe to the output_path
    if output_path is not None:
        dfImmigration.select(columns).write.save(output_path, mode="overwrite", format="parquet", partitionBy = partitionBy)
    return dfImmigration



def etl_states(spark, input_path="us-cities-demographics.csv", output_path=None, 
                         input_format = "csv", columns='*', load_size = None, partitionBy = ["StateCode"], sep=";", **options):
    """
    Reads the cities demographics dataset from the input_path, transforms it to the state dimension table and saves it to the output_path.
    
    Args:
        spark (SparkSession): Spark session
        input_path (string): Directory with the input files
        output_path (string): Directory to save immigration output files
        input_format (str): Format of the input files, default to "csv"
        columns (list): List of the columns names to read in
        load_size (int): Number of rows to read for debug purposes
        partitionBy (list): Files will be saved in partitions using the columns of this list
        sep (string): Separator for reading the csv-file
        options: All other string options
    """
    
    # Loads the demografics dataframe using Spark
    dfDemogr = spark.read.csv(input_path, inferSchema=True, header=True, sep=sep)
    
    # Aggregates some values to the city level: Since those values are repeated in every row with a certain city, we take the min
    dfDemogrAggr = dfDemogr.groupby(["City","State","State Code"]).min()\
        .withColumnRenamed('min(Median Age)', 'MedianAge')\
        .withColumnRenamed('min(Average Household Size)', 'AverageHouseholdSize')\
        .withColumnRenamed('min(Male Population)', 'MalePopulation')\
        .withColumnRenamed('min(Female Population)', 'FemalePopulation')\
        .withColumnRenamed('min(Total Population)', 'TotalPopulation')\
        .withColumnRenamed('min(Number of Veterans)', 'NumberOfVeterans')\
        .withColumnRenamed('min(Foreign-born)', 'ForeignBorn')\
        .select(["City","State","State Code","MedianAge","AverageHouseholdSize","MalePopulation","FemalePopulation",\
                 "TotalPopulation","NumberOfVeterans","ForeignBorn"])
    
    # Creates a pivot for race to aggregate the numbers to one row for each city
    dfDemogrPiv = dfDemogr.groupBy(["City", "State", "State Code"]).pivot("Race").sum("Count")\
        .withColumnRenamed('American Indian and Alaska Native', 'AmericanIndianAndAlaskaNative')\
        .withColumnRenamed('Black or African-American', 'BlackOrAfricanAmerican')\
        .withColumnRenamed('Hispanic or Latino', 'HispanicOrLatino')
    
    # Joins both tables
    dfDemogrF = dfDemogrAggr.join(other=dfDemogrPiv, on=["City", "State", "State Code"], how="inner")\
        .withColumnRenamed('State Code', 'StateCode')
    
    # Replaces missing values with 0
    dfDemogrF = dfDemogrF.fillna(0, ['MedianAge', 'AverageHouseholdSize', 'MalePopulation', 'FemalePopulation', 'TotalPopulation', 'NumberOfVeterans', 'ForeignBorn',
                                     'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican', 'HispanicOrLatino', 'White'])
    
    # Converts columns to integer
    dfDemogrF = dfDemogrF.withColumn('MalePopulation', dfDemogrF['MalePopulation'].cast(IntegerType()))\
        .withColumn('FemalePopulation', dfDemogrF['FemalePopulation'].cast(IntegerType()))\
        .withColumn('TotalPopulation', dfDemogrF['TotalPopulation'].cast(IntegerType()))\
        .withColumn('NumberOfVeterans', dfDemogrF['NumberOfVeterans'].cast(IntegerType()))\
        .withColumn('ForeignBorn', dfDemogrF['ForeignBorn'].cast(IntegerType()))\
        .withColumn('AmericanIndianAndAlaskaNative', dfDemogrF['AmericanIndianAndAlaskaNative'].cast(IntegerType()))\
        .withColumn('Asian', dfDemogrF['Asian'].cast(IntegerType()))\
        .withColumn('BlackOrAfricanAmerican', dfDemogrF['BlackOrAfricanAmerican'].cast(IntegerType()))\
        .withColumn('HispanicOrLatino', dfDemogrF['HispanicOrLatino'].cast(IntegerType()))\
        .withColumn('White', dfDemogrF['White'].cast(IntegerType()))
    
    # To aggregate the columns to the state level we have to recalculate the average considering number of total population in every city
    dfDemogrAvg = dfDemogrF.withColumn("MedianAgeSum", dfDemogrF.MedianAge * dfDemogrF.TotalPopulation)\
        .withColumn("AverageHouseholdSizeSum", dfDemogrF.AverageHouseholdSize * dfDemogrF.TotalPopulation)  
    dfDemogrSum = dfDemogrAvg.groupby(["State","StateCode"]).sum()\
        .select(["State","StateCode","sum(TotalPopulation)","sum(MalePopulation)","sum(FemalePopulation)","sum(ForeignBorn)","sum(NumberOfVeterans)",
                 "sum(AmericanIndianAndAlaskaNative)","sum(Asian)","sum(BlackOrAfricanAmerican)","sum(HispanicOrLatino)","sum(White)",
                 "sum(MedianAgeSum)","sum(AverageHouseholdSizeSum)"])
    dfDemogrState = dfDemogrSum.withColumn("MedianAgeNew", (dfDemogrSum["sum(MedianAgeSum)"]/dfDemogrSum["sum(TotalPopulation)"]).cast(DoubleType()))\
        .withColumn("AverageHouseholdSizeNew", (dfDemogrSum["sum(AverageHouseholdSizeSum)"]/dfDemogrSum["sum(TotalPopulation)"]).cast(DoubleType()))
    
    # Renames the columns
    dfDemogrState = dfDemogrState.withColumnRenamed("sum(MalePopulation)", "MalePopulation")\
        .withColumnRenamed("sum(FemalePopulation)", "FemalePopulation")\
        .withColumnRenamed("sum(TotalPopulation)", "TotalPopulation")\
        .withColumnRenamed("sum(ForeignBorn)", "ForeignBorn")\
        .withColumnRenamed("sum(NumberOfVeterans)", "NumberOfVeterans")\
        .withColumnRenamed("sum(AmericanIndianAndAlaskaNative)", "AmericanIndianAndAlaskaNative")\
        .withColumnRenamed("sum(Asian)", "Asian")\
        .withColumnRenamed("sum(BlackOrAfricanAmerican)", "BlackOrAfricanAmerican")\
        .withColumnRenamed("sum(HispanicOrLatino)", "HispanicOrLatino")\
        .withColumnRenamed("sum(White)", "White")
    
    dfState = dfDemogrState.drop("sum(MedianAgeSum)","sum(AverageHouseholdSizeSum)")\
        .withColumnRenamed("MedianAgeNew", "MedianAge")\
        .withColumnRenamed("AverageHouseholdSizeNew", "AverageHouseholdSize")
    
    # Saves the state dataframe to the output_path
    if output_path is not None:
        dfState.select(columns).write.save(output_path, mode="overwrite", format="parquet", partitionBy = partitionBy)
    return dfState



def etl_countries(spark, input_path="../../data2/GlobalLandTemperaturesByCity.csv", output_path=None, 
                         input_format = "csv", columns = '*', load_size = 1000, partitionBy = ["CountryName"], **options):
    """
    Reads the temperature dataset from the input_path, transforms it to the country dimension table and saves it to the output_path.
    
    Args:
        spark (SparkSession): Spark session
        input_path (string): Directory with the input files
        output_path (string): Directory to save immigration output files
        input_format (str): Format of the input files, default to "csv"
        columns (list): List of the columns names to read in
        load_size (int): Number of rows to read for debug purposes
        partitionBy (list): Files will be saved in partitions using the columns of this list
        options: All other string options
    """
    
    # Loads the temperature dataframe using Spark
    dfTemp = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Aggregates the dataset by country and renames the name of new columns
    dfTempAvg = dfTemp.groupby(["Country"]).avg()\
        .select(["Country","avg(AverageTemperature)"])\
        .withColumnRenamed('Country','CountryName')\
        .withColumnRenamed('avg(AverageTemperature)', 'Temperature')
    
    # Renames some country found in the data analysis to enable a join with the country code list
    dfTempAvg = dfTempAvg.withColumn('CountryName', when(dfTempAvg['CountryName'] == 'Congo (Democratic Republic Of The', 'Congo').otherwise(dfTempAvg['CountryName']))
    dfTempAvg = dfTempAvg.withColumn('CountryName', when(dfTempAvg['CountryName'] == "Côte D'Ivoire", 'Ivory Coast').otherwise(dfTempAvg['CountryName']))
    
    # Creates a column with the country name in all lower letters to enable the join with the country code list
    dfTempAvg = dfTempAvg.withColumn('Country_Lower', lower(dfTempAvg.CountryName))
    
    # Loads the table of countries and their country code
    i94_country = spark.read.load("I94_Country.csv", format="csv", sep=";", header=True).select(['Code','Country'])
    
    # Renames some country found in the data analysis to enable a join with the countries from the temperature data frame
    i94_country = i94_country.withColumn('Country', when(i94_country['Country'] == 'BOSNIA-HERZEGOVINA', 'BOSNIA AND HERZEGOVINA').otherwise(i94_country['Country']))
    i94_country = i94_country.withColumn('Country', when(i94_country['Country'] == "INVALID: CANADA", 'CANADA').otherwise(i94_country['Country']))
    i94_country = i94_country.withColumn('Country', when(i94_country['Country'] == "CHINA, PRC", 'CHINA').otherwise(i94_country['Country']))
    i94_country = i94_country.withColumn('Country', when(i94_country['Country'] == "GUINEA-BISSAU", 'GUINEA BISSAU').otherwise(i94_country['Country']))
    i94_country = i94_country.withColumn('Country', when(i94_country['Country'] == "INVALID: PUERTO RICO", 'PUERTO RICO').otherwise(i94_country['Country']))
    i94_country = i94_country.withColumn('Country', when(i94_country['Country'] == "INVALID: UNITED STATES", 'UNITED STATES').otherwise(i94_country['Country']))
    i94_country = i94_country.withColumn('Country', when(i94_country['Country'] == "MEXICO Air Sea, and Not Reported (I-94, no land arrivals)", 'MEXICO').otherwise(i94_country['Country']))
    
    # Creates a column with the country name in all lower letters to enable the join with the country code list
    i94_country = i94_country.withColumn('i94_Country_Lower', lower(i94_country.Country))
    i94_country = i94_country.withColumn('Code', i94_country['Code'].cast(IntegerType()))
    
    # Joins both country tables to create a country dimension table
    i94_country = i94_country.join(dfTempAvg, i94_country.i94_Country_Lower == dfTempAvg.Country_Lower, how="left")
    
    dfCountry = i94_country.withColumn("CountryName1", initcap(col("i94_Country_Lower")))\
        .select(["Code","CountryName1","Temperature"])\
        .withColumnRenamed("CountryName1","CountryName")
    
    # Saves the country dataframe to the output_path
    if output_path is not None:
        dfCountry.select(columns).write.save(output_path, mode="overwrite", format="parquet", partitionBy = partitionBy)
    return dfCountry



def date_diff(date1, date2):
    '''
    Calculates the difference in days between two dates
    '''
    
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, "%Y-%m-%d")
        b = datetime.strptime(date2, "%Y-%m-%d")
        delta = b - a
        return delta.days



# User defined functions using Spark udf wrapper function for date operations
convert_sas_udf = udf(lambda x: x if x is None else (datetime(1960,1,1) + timedelta(days=x)).strftime("%Y-%m-%d"))
date_diff_udf = udf(date_diff)



if __name__ == "__main__" :
    spark = create_spark_session()
    
    # Perform ETL process to the immigration data and load it into s3 bucket
    immigration = etl_immigration(spark, input_path='../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat',
                                     output_path=output_data+"immigration.parquet",
                                     date_output_path=output_data+"date.parquet",
                                     input_format = "com.github.saurfang.sas.spark", 
                                     load_size=None, partitionBy=None)
    
    # Perform ETL process to the immigration data and load it into s3 bucket
    countries = etl_countries(spark, output_path=output_data + "country.parquet")
    
    # Perform ETL process to the immigration data and load it into s3 bucket
    states = etl_states(spark, output_path=output_data + "state.parquet")
    