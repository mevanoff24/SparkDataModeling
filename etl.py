import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType, IntegerType, FloatType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def update_dtype(df, column, dtype):
    """
    Updates data type for column
    Args:
        df (spark DataFrame): DataFrame to update column for
        column (str): Column name
        dtype (object): Spark SQL data type
    Returns:
        df (spark DataFrame): updated DataFrame
    """
    return df.withColumn(column, df[column].cast(dtype))


def create_spark_session():
    """
    Create Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Spark pipeline to process and save json formatted song data 
    Saves to parquet file type on S3
    Args:
        spark (object): Spark session
        input_data (str): S3 bucket input name
        output_data (str): S3 bucket output name
    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'artist_name', 'year', 'duration']].drop_duplicates()
    # cast year and duration to integer and float
    songs_table = update_dtype(songs_table, 'year', IntegerType())
    songs_table = update_dtype(songs_table, 'duration', FloatType())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'song_data', mode='overwrite', partitionBy=('year', 'artist_name'))

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 
                        'artist_latitude', 'artist_longitude']].drop_duplicates()
    # cast latitude and longitude to double
    for column in ['artist_latitude', 'artist_longitude']:
        artists_table = update_dtype(artists_table, column, DoubleType())
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artist_data', mode='overwrite')
    

    
def process_log_data(spark, input_data, output_data):
    """
    Spark pipeline to process and save csv formatted log data 
    Saves to parquet file type on S3
    Args:
        spark (object): Spark session
        input_data (str): S3 bucket input name
        output_data (str): S3 bucket output name
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'user_data', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x) / 1000.), returnType=TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x) / 1000.), returnType=DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # add year and month -- needed for songplays_table partitione
    df = df.withColumn("year", year(df.datetime).alias('year'))
    df = df.withColumn("month", month(df.datetime).alias('month'))
    
    # extract columns to create time table
    time_table = df.select('start_time', 'year', 'month',
                       hour(df.datetime).alias('hour'),
                       dayofmonth(df.datetime).alias('dayofmonth'),
                       weekofyear(df.datetime).alias('weekofyear'),
                       dayofweek(df.datetime).alias('weekday')).drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time_data', mode='overwrite', partitionBy=('year', 'month'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'song_data')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df[['start_time', 'userId', 'level', 'sessionId', 'location', 'userAgent', 
                                                                  'song', 'length', 'year', 'month']] \
        .join(song_df[['song_id', 'artist_id', 'title', 'duration']], 
            on = (df.song == song_df.title) & (df.length == song_df.duration), how = 'inner') \
        .withColumn('songplay_id', monotonically_increasing_id()) \
        .select('songplay_id', 'start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 
                                                            'location', 'userAgent', 'year', 'month') \
        .drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays_data', mode='overwrite', partitionBy=('year', 'month'))
    

def main():
    """
    Main Function
    Loads spark context
    Defines input and output S3 bucket names
    Processed and saves song and log data
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/analytics"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
