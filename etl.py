import configparser
import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType , StructField, StructType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import udf, col, lit, when, year, month, upper, to_date, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.functions import monotonically_increasing_id

# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS configuration
config = configparser.ConfigParser()
config.read('dwh.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"


# data processing function to create Spark session
def create_spark_session():
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .enableHiveSupport() \
    .getOrCreate()
    return spark

#Function to convert to Dateformat
def SAS_to_date_format(date):
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
SAS_to_date_udf = udf(SAS_to_date_format, DateType())


#Function to rename columns
def rename_columns(table, new_columns):
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table


def processing_immigration_data(spark, input_data, output_data):
    """Process Immigration data to get immigration_data, immigration_personal and immigration_airline tables.
    Arguments:
        spark {object}: SparkSession object
        input_data {object}: Source S3 bucket
        output_data {object}: Destintion S3 bucket

    Returns: None
    """

    logging.info("Start processing immigration")
    
    #immigration_data = os.path.join(input_data + 'immigration/18-83510-I94-Data-2016/i94_feb16_sub.sas7bdat')
    #df_spark = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data)
    
    # read immigration data file
    df_spark =spark.read.load('./sas_data')
    
    logging.info("Start processing immigration table")
    # extracting columns to create immigration table
    immigration_data = df_spark.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr',\
                                       'arrdate', 'depdate', 'i94mode', 'i94visa').distinct()\
                                        .withColumn("immigration_id", monotonically_increasing_id())
    
    #Renaming columns of immigration_data table
    new_columns = ['cic_id', 'year', 'month', 'city_code', 'state_code','arrival_date', \
                   'departure_date', 'mode', 'visa']
    immigration_data = rename_columns(immigration_data, new_columns)

    
    immigration_data = immigration_data.withColumn('country', lit('United States'))
    immigration_data = immigration_data.withColumn('arrival_date', SAS_to_date_udf(col('arrival_date')))                            
    immigration_data = immigration_data.withColumn('departure_date', SAS_to_date_udf(col('departure_date')))
    logging.info("data wrangling completed by adding columns, country as United States ,arrival_date, departure_date")

    # write immigration table to parquet files partitioned by state_code
    immigration_data.write.mode("overwrite").partitionBy('state_code')\
                .parquet(path= output_data + 'immigration_data')         
    logging.info("Created parquet files from immigration table, partitioned by state")
    
    #Processing for immigration_date season table
    immigration_data = immigration_data.withColumn('arrival_month',month(immigration_data.arrival_date))
    immigration_data = immigration_data.withColumn('arrival_year',year(immigration_data.arrival_date))
    immigration_data = immigration_data.withColumn('arrival_day',dayofmonth(immigration_data.arrival_date))
    immigration_data = immigration_data.withColumn('day_of_week',dayofweek(immigration_data.arrival_date))
    immigration_data = immigration_data.withColumn('arrival_week_of_year',weekofyear(immigration_data.arrival_date))
    
    # extracting columns to create immigration_date table
    immigration_date = immigration_data.select('arrival_date','arrival_month','day_of_week','arrival_year',\
                                           'arrival_day','arrival_week_of_year').dropDuplicates()
    
    # Create temporary sql table
    immigration_date.createOrReplaceTempView("immigration_date")
    
    # Add seasons to immigration_date dimension table
    immigration_date_season=spark.sql('''SELECT arrival_date,
                         arrival_month,
                         day_of_week,
                         arrival_year,
                         arrival_day,
                         arrival_week_of_year,
                         CASE WHEN arrival_month IN (12, 1, 2) THEN 'winter' 
                                WHEN arrival_month IN (3, 4, 5) THEN 'spring' 
                                WHEN arrival_month IN (6, 7, 8) THEN 'summer' 
                                ELSE 'autumn' 
                         END AS date_season from immigration_date''')
    
    
    # write immigration_date dimension table to parquet file partitioned by year and month
    immigration_date_season.write.mode("overwrite")\
                            .partitionBy("arrival_year", "arrival_month").parquet(path= output_data + 'immigration_date')
    logging.info("Created parquet files from immigration_date_season table.")
    

    logging.info("Start processing immigration_personal")
    # extract columns to create immigration_personal table
    immigration_personal = df_spark.select('cicid', 'i94cit', 'i94res', 'biryear', 'gender', 'insnum').distinct()\
                                            .withColumn("immi_personal_id", monotonically_increasing_id())
    
    #Renaming columns of immigration_personal table
    new_columns = ['cic_id', 'citizen_country', 'residence_country',\
                   'birth_year', 'gender', 'ins_num']
    immigration_personal = rename_columns(immigration_personal, new_columns)
    

    # write immigration_personal table to parquet files
    immigration_personal.write.mode("overwrite")\
                        .parquet(path = output_data + 'immigration_personal')
    logging.info("Created parquet files from immigration_personal table.")
    
    logging.info("Start processing immigration_airline table")
    # extract columns to create immigration_airline table
    immigration_airline = df_spark.select('cicid', 'airline', 'admnum', 'fltno', 'visatype').distinct()\
                                .withColumn("immi_airline_id", monotonically_increasing_id())
    
    
    # Renaming columns of immigration_airline table
    new_columns = ['cic_id', 'airline', 'admin_num', 'flight_number', 'visa_type']
    immigration_airline = rename_columns(immigration_airline, new_columns)

    # write immigration_airline table to parquet files
    immigration_airline.write.mode("overwrite")\
                        .parquet(path=output_data + 'immigration_airline')
    logging.info("Created parquet files from immigration_airline table.")


def processing_label_descriptions(spark, input_data, output_data):
    """ Parsing label description file to get codes of country, state and city
    Arguments:
        spark {object}: SparkSession object
        input_data {object}: Source S3 bucket
        output_data {object}: Destintion S3 bucket

    Returns: None
    """

    logging.info("Start processing label descriptions")
    label_file = os.path.join("./I94_SAS_Labels_Descriptions.SAS")
    with open(label_file) as f:
        contents = f.readlines()

    country_code = {}
    for countries in contents[10:298]:
        set = countries.split('=')
        code, country = set[0].strip(), set[1].strip().strip("'")
        country_code[code] = country
    spark.createDataFrame(country_code.items(), ['code', 'country'])\
        .write.mode("overwrite")\
        .parquet(path = output_data + 'country_code')
    logging.info("Created country_code parquet files")

    city_code = {}
    for cities in contents[303:962]:
        set = cities.split('=')
        code, city = set[0].strip("\t").strip().strip("'"),\
        set[1].strip('\t').strip().strip("''")
        city_code[code] = city
    spark.createDataFrame(city_code.items(), ['code', 'city'])\
        .write.mode("overwrite")\
        .parquet(path = output_data + 'city_code')
    logging.info("Created city_code parquet files")

    state_code = {}
    for states in contents[982:1036]:
        set = states.split('=')
        code, state = set[0].strip('\t').strip("'"), set[1].strip().strip("'")
        state_code[code] = state
    spark.createDataFrame(state_code.items(), ['code', 'state'])\
        .write.mode("overwrite")\
        .parquet(path = output_data + 'state_code')
    logging.info("Created state_code parquet files")



def processing_temperature_data(spark, input_data, output_data):
    """ Process temperature data to get temperature_data table.
    Arguments:
        spark {object}: SparkSession object
        input_data {object}: Source S3 bucket
        output_data {object}: Destintion S3 bucket

    Returns: None
    """

    logging.info("Start processing temperature_data")
    # Reads temperature data file
    temperature_data = os.path.join(input_data + 'GlobalLandTemperaturesByCity.csv')
    df = spark.read.csv(temperature_data, header=True)
    

    df = df.where(df['Country'] == 'United States')
    temperature_data = df.select(['dt', 'AverageTemperature', 'AverageTemperatureUncertainty',\
                                  'City', 'Country']).distinct()

    # Renaming columns of temperature_data table
    new_columns = ['dt', 'avg_temp', 'avg_temp_uncertainty', 'city', 'country']
    temperature_data = rename_columns(temperature_data, new_columns)

    temperature_data = temperature_data.withColumn('dt', to_date(col('dt')))
    temperature_data = temperature_data.withColumn('year', year(temperature_data['dt']))
    temperature_data = temperature_data.withColumn('month', month(temperature_data['dt']))

    # write temperature_data table to parquet files
    temperature_data.write.mode("overwrite")\
                    .parquet(path=output_data + 'temperature_data')
    logging.info("Created temperature_data parquet files")


def processing_demographics_data(spark, input_data, output_data):
    """ Process demographics data to get demographics_population and demographics_stats table.

    Arguments:
        spark {object}: SparkSession object
        input_data {object}: Source S3 bucket
        output_data {object}: Destintion S3 bucket

    Returns: None
    """

    logging.info("Start processing demographics_data")
    # read demographics_data file
    demographics_data = os.path.join(input_data + 'us-cities-demographics.csv')
    df = spark.read.format('csv').options(header=True, delimiter=';').load(demographics_data)

    
    demographics_population = df.select(['City', 'State', 'Male Population', 'Female Population', \
                              'Number of Veterans', 'Foreign-born', 'Race']).distinct().na.drop() \
                              .withColumn("demog_pop_id", monotonically_increasing_id())
    
    # Renaming columns of demographics_population table
    new_columns = ['city', 'state', 'male_population', 'female_population', \
               'num_of_vetarans', 'foreign_born', 'race']
    demographics_population = rename_columns(demographics_population, new_columns)

    
    # write demographics_population table to parquet files
    demographics_population.write.mode("overwrite")\
                            .parquet(path=output_data + 'dim_demog_population')
    logging.info("Created demographics_population parquet files")

    logging.info("Start processing demographics_stats")
    demographics_stats = df.select(['City', 'State', 'Median Age', 'Average Household Size'])\
                            .distinct().na.drop().withColumn("demog_stat_id", monotonically_increasing_id())
                            
    # Renaming columns of demographics_stats table
    new_columns = ['city', 'state', 'median_age', 'avg_household_size']
    demographics_stats = rename_columns(demographics_stats, new_columns)
    # Converting all data in city and state columns to Uppercase.
    demographics_stats = demographics_stats.withColumn('city', upper(col('city')))
    demographics_stats = demographics_stats.withColumn('state', upper(col('state')))
    
    # write demographics_stats table to parquet files
    demographics_stats.write.mode("overwrite")\
                        .parquet(path = output_data + 'demographics_stats')
    logging.info("Created demographics_stats parquet files")

    
def main():
    spark = create_spark_session()
    input_data = 's3a://source-bucket-1726/'
    output_data = 's3a://destination-bucket-1726/'
    
    processing_immigration_data(spark, input_data, output_data)    
    processing_label_descriptions(spark, input_data, output_data)
    processing_temperature_data(spark, input_data, output_data)
    processing_demographics_data(spark, input_data, output_data)
    logging.info("Data processing completed")


if __name__ == "__main__":
    main()