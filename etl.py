"""
This module includes the ETL process

Author: Fabio Barbazza
Date: Nov, 2022
"""
import logging 

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        This method create an instance of SparkSession

        Returns:
            spark(SparkSession)
    """
    try:

        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        return spark
    
    except Exception as err:
        logger.exception(err)
        raise err


def process_song_data(spark, input_data, output_data):
    """
        This method is responsible for processing song data.
        It stores songs and artists tables
    """
    try:
        # get filepath to song data file
        song_data_path = os.path.join(input_data,'song_data','*','*','*','*.json')
        
        logger.info('song_data_path: {}'.format(song_data_path))

        # read song data file
        df_song = spark.read.json(song_data_path)

        # extract columns to create songs table (song_id, title, artist_id, year, duration)
        songs_table = df_song.select(['song_id','title','artist_id','year','duration'])
        
        # write songs table to parquet files partitioned by year and artist
        song_table_path = os.path.join(output_data,'songs.parquet')

        logger.info('song_table_path: {}'.format(song_table_path))

        songs_table.write.parquet(song_table_path)

        # extract columns for artist table  (artist_id, name, location, lattitude, longitude) 
        artists_table = df_song.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
        
        # write artist table to parquet files
        artist_table_path = os.path.join(output_data,'artist.parquet')

        logger.info('artist_table_path: {}'.format(artist_table_path))

        artists_table.write.mode('overwrite').parquet(artist_table_path)

        logger.info('SUCCESS')

    except Exception as err:
        logger.exception(err)
        raise err


def process_log_data(spark, input_data, output_data):
    """
        This method is responsible for processing song data
    """
    try:
        # get filepath to log data file
        log_data = os.path.join(input_data,'log_data','*.json')

        logger.info('log_data path: {}'.format(log_data))

        # read log data file
        df_log = spark.read.json(log_data)
        
        # filter by actions for song plays
        df_log_next_song = df_log.filter(df_log.page == 'NextSong')

        # extract columns for user table (user_id, first_name, last_name, gender, level)
        df_users = df_log_next_song.select(['userId','firstName','lastName','gender','level'])

        # write users table to parquet files
        user_table_path = os.path.join(output_data,'users.parquet')

        logger.info('user_table_path: {}'.format(user_table_path))

        df_users.write.parquet(user_table_path)


        # create timestamp column from original timestamp column
        get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
        df_log_next_song = df_log_next_song.withColumn("ts_timestamp",get_timestamp("ts"))

        
        df_log_next_song = df_log_next_song.withColumn('ts_year',year(df_log_next_song.ts_timestamp))
        df_log_next_song = df_log_next_song.withColumn('ts_month',month(df_log_next_song.ts_timestamp))
        df_log_next_song = df_log_next_song.withColumn('ts_day',dayofmonth(df_log_next_song.ts_timestamp))
        df_log_next_song = df_log_next_song.withColumn('ts_weekofyear',weekofyear(df_log_next_song.ts_timestamp))
        df_log_next_song = df_log_next_song.withColumn('ts_hour',hour(df_log_next_song.ts_timestamp))
        df_log_next_song = df_log_next_song.withColumn('ts_weekday',weekofyear(df_log_next_song.ts_timestamp))

        time_table = df_log_next_song.select(['ts_timestamp','ts_hour','ts_day','ts_weekofyear','ts_month','ts_weekday'])
        
        # write time table to parquet files partitioned by year and month
        time_table_path = os.path.join(output_data,'time.parquet')

        logger.info('time_table_path: {}'.format(time_table_path))

        time_table.write.mode('overwrite').parquet(time_table_path)

        # read in song data to use for songplays table
        song_path = os.path.join('artifacts','songs.parquet')
        song_df = spark.read.parquet(song_path)

        # create temp view to use SQL
        song_df.createOrReplaceTempView('songs_staging')
        df_log_next_song.createOrReplaceTempView('logs_staging')

        # extract columns from joined song and log datasets to create songplays table (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        songplays_table = spark.sql("""
            select 
                logs_staging.registration,
                logs_staging.userId,
                logs_staging.level,
                songs_staging.song_id,
                songs_staging.artist_id,
                logs_staging.sessionId,
                logs_staging.location,
                logs_staging.userAgent
            from logs_staging
            left join songs_staging
            on logs_staging.artist=songplays_table.artist_name
        """)

        song_play_path = os.path.join(output_data,'song_play.parquet')

        logger.info('song_play_path: {}'.format(song_play_path))

        # write songplays table to parquet files partitioned by year and month
        songplays_table.write.mode('overwrite').partitionBy("level").parquet(song_play_path)


    except Exception as err:
        logger.exception(err)
        raise err


def main():
    """
        This is the entrypoint of the module
    """
    try:
        logger.info('starting')

        spark = create_spark_session()
        input_data = "s3a://udacity-dend"
        output_data = "s3a://udacity-dend/artifacts"
        
        process_song_data(spark, input_data, output_data)    
        process_log_data(spark, input_data, output_data)

        logger.info('success')

    except Exception as err:
        logger.exception(err)
        raise err


if __name__ == "__main__":
    main()
