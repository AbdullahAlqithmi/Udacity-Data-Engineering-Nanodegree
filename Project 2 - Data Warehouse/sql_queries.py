import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_Events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_Songs"
songplay_table_drop = "DROP TABLE IF EXISTS songPlays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_Events 
(
    artist          VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER,
    last_name       VARCHAR,
    length          DECIMAL,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    DECIMAL,
    session_id      INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    user_agent      VARCHAR,
    user_id         INTEGER
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_Songs
( 
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  DECIMAL,
    artist_longitude DECIMAL,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DECIMAL,
    year             INTEGER
    );
""")

songplay_table_create = ("""
 CREATE TABLE songplays(
        songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INTEGER ,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR ,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    );
""")




user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id     INTEGER  PRIMARY KEY, 
    first_name  VARCHAR, 
    last_name   VARCHAR, 
    gender      VARCHAR,
    level       VARCHAR
    );
""")
     


song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
    song_id    VARCHAR  PRIMARY KEY, 
    title      VARCHAR, 
    artist_id  VARCHAR,
    year       INTEGER, 
    duration   FLOAT
    );
""")
     

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
    artist_id  VARCHAR  PRIMARY KEY,
    name       VARCHAR, 
    location   VARCHAR, 
    latitude   FLOAT, 
    longitude  FLOAT
    ) ;
""")

     
time_table_create = ("""
CREATE TABLE time(
        start_time          TIMESTAMP       NOT NULL PRIMARY KEY,
        hour                INTEGER         NOT NULL,
        day                 INTEGER         NOT NULL,
        week                INTEGER         NOT NULL,
        month               INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
    ) ;
""")



# STAGING TABLES

staging_events_copy = ("""
    copy staging_Events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])



staging_songs_copy = ("""
    copy staging_Songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])





# FINAL TABLES


songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(ev.ts)  AS start_time, 
            ev.user_id        AS user_id, 
            ev.level          AS level, 
            so.song_id       AS song_id, 
            so.artist_id     AS artist_id, 
            ev.session_id     AS session_id, 
            ev.location      AS location, 
            ev.user_agent     AS user_agent
    FROM staging_events ev
    JOIN staging_songs  so   ON (ev.song = so.title AND ev.artist = so.artist_name)
    WHERE ev.page = 'NextSong'
""")


user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id,
    first_name,
    last_name,
    gender, 
    level
    FROM staging_Events
    WHERE page = 'NextSong'
    ;""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
    title, 
    artist_id,
    year,
    duration
    FROM staging_Songs
    ;""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_Songs
    ;""")



time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time, 
    EXTRACT(hour from start_time)   as hour , 
    EXTRACT(day from start_time)    as day  ,
    EXTRACT(week from start_time)   as week ,
    EXTRACT(month from start_time)  as month,
    EXTRACT(year from start_time)   as year ,
    EXTRACT(weekday from start_time)as weekday
    FROM songplays;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ songplay_table_insert,user_table_insert,song_table_insert,artist_table_insert,time_table_insert]
