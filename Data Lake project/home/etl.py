import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a spark session
    
    Returns:
    spark session
    
    """
    
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process Song data read from input_data folder and write to out_data folder with spark session
  

  
    Parameters: 
        spark: spark session
        input_data: input data folder
        output_data: output data folder
  
      
   Returns: 
       
    """
  
    
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title','artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+"songs", mode="overwrite", partitionBy =['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude').dropDuplicates() 
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists", mode="overwrite")

# create timestamp column from original timestamp column
def convert_ts(ts):
    """
    Convert bigint type to timestamptype
    
    Parameters:
        ts(bigint): timestamp in bigint type
    
    Returns:
        ts(timestamp): timestamp in timestamp type
    
    
    """
        
    return datetime.utcfromtimestamp(int(ts)/1000)
	
	
def process_log_data(spark, input_data, output_data):
     """
    Process log data read from input_data folder and write to out_data folder with spark session
   
    Parameters: 
        spark: spark session
        input_data: input data folder
        output_data: output data folder
  
      
   Returns: 
       
    """
    
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender','level').dropDuplicates() 
    
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"users", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts : convert_ts(ts), TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
        
    # extract columns to create time table
    time_table = df.withColumn('start_time', df.timestamp)\
    .withColumn('hour', hour(df.timestamp))\
    .withColumn('day', dayofmonth(df.timestamp))\
    .withColumn('week', weekofyear(df.timestamp))\
    .withColumn('month', month(df.timestamp))\
    .withColumn('year', year(df.timestamp))\
    .withColumn('weekday', dayofweek(df.timestamp)).select('start_time','hour','day','week','month','year','weekday').dropDuplicates() 
    

 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+"time", mode="overwrite", partitionBy =['year', 'month'])

    # read in song data to use for songplays table
    song_df = input_data + "song_data/*/*/*"

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title)\
            .select(F.monotonically_increasing_id().alias('songplay_id')\
            ,df.timestamp.alias('start_time'),df.userId.alias('user_id')\
            ,df.level\
            ,song_df.song_id\
            ,song_df.artist_id\
            ,df.sessionId.alias('session_id')\
            ,df.location\
            ,df.userAgent)


    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn('year',year(col("start_time")))\
                .withColumn('month', month(col('start_time')))\
                .write.parquet(output_data+"songplays", mode="overwrite", partitionBy =['year', 'month'])

                


	
	
# need to create buckets in your S3 for output_data
def main():
    """
    Main function
    """
    
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-spark-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
