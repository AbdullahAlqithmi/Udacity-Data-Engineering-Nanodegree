import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs")
    songs_table =  spark.sql("""
    SELECT distinct 
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
     songs_table.write.partitionBy('year', 'artist_id') .parquet(os.path.join(output_data,'songs/songs.parquet'),'overwrite')
    
    # extract columns to create artists table
    df.createOrReplaceTempView("artists")
    artists_table = spark.sql("""
       SELECT distinct
       artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
       FROM artists 
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data + "log_data/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("songplays")
    df = spark.sql("""
   SELECT *
   FROM songplays
   where page = 'NextSong
    """)

    # extract columns for users table  
    df.createOrReplaceTempView("users")
    users_table = spark.sql("""
    SELECT
    userId,
    firstName,
    lastName,
    gender,
    level
    FROM users 
    """).dropDuplicates(['userId'])

        
       
        
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')
   
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    df.createOrReplaceTempView("time")
    time_table = park.sql(("""
    SELECT distinct 
    timestamp as start_time, 
    hour(timestamp) as hour, 
    day(timestamp) as day, 
    weekofyear(timestamp) as week, 
    month(timestamp) as month, 
    year(timestamp) as year, 
    weekday(timestamp) as weekday
    from time 
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') .parquet(os.path.join(output_data,'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    select 
    a.timestamp as start_time,
    a.userId,
    a.level,
    b.song_id,
    b.artist_id,
    a.sessionId, 
    a.location,
    a.userAgent,
    year(a.timestamp) as year,
    month(a.timestamp) as month 
    from artists as a 
    inner join songs as b on a.song = b.song_title
    """)

    # write songplays table to parquet files partitioned by year and month
songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4dend-Alqithmi/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
