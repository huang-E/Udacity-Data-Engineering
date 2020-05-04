import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES removing length to avoid the length issue



staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
             ( 
              artist        VARCHAR, 
              auth          VARCHAR, 
              firstname     VARCHAR, 
              gender        VARCHAR, 
              iteminsession INT, 
              lastname      VARCHAR, 
              length        FLOAT, 
              level         VARCHAR, 
              location      VARCHAR, 
              method        VARCHAR, 
              page          VARCHAR, 
              registration  BIGINT, 
              sessionid     INT, 
              song          VARCHAR, 
              status        INT, 
              ts            TIMESTAMP, 
              useragent     VARCHAR, 
              userid        INT
               );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
          ( 
             num_songs        INT, 
             artist_id        VARCHAR, 
             artist_latitude  FLOAT, 
             artist_longitude FLOAT, 
             artist_location  VARCHAR, 
             artist_name      VARCHAR, 
             song_id          VARCHAR, 
             title            VARCHAR, 
             duration         FLOAT, 
             year             INT 
          );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay 
             ( 
             songplay_id INT identity(0,1) PRIMARY KEY, 
             start_time timestamp NOT NULL REFERENCES dim_time (start_time), 
             user_id    INT NOT NULL NOT NULL REFERENCES dim_users (user_id), 
             level      VARCHAR, 
             song_id    VARCHAR NOT NULL REFERENCES dim_songs (song_id), 
             artist_id  VARCHAR NOT NULL REFERENCES dim_artists (artist_id), 
             session_id INT NOT NULL, 
             location   VARCHAR, 
             user_agent VARCHAR 
           );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users 
          ( 
             user_id    INT PRIMARY KEY, 
             first_name VARCHAR, 
             last_name  VARCHAR, 
             gender     VARCHAR, 
             level      VARCHAR 
          ) ;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs 
             ( 
                          song_id   VARCHAR PRIMARY KEY , 
                          title     VARCHAR , 
                          artist_id VARCHAR NOT NULL REFERENCES dim_artists(artist_id) , 
                          year      INT , 
                          duration FLOAT
             );

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists 
  ( 
     artist_id VARCHAR PRIMARY KEY, 
     name      VARCHAR, 
     location  VARCHAR, 
     latitude FLOAT, 
     longitude FLOAT 
  );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time 
  ( 
     start_time TIMESTAMP PRIMARY KEY, 
     hour       INT NOT NULL, 
     day        INT NOT NULL, 
     week       INT NOT NULL, 
     month      INT NOT NULL, 
     year       INT NOT NULL, 
     weekday    INT NOT NULL 
  ) 
""")

# STAGING TABLES

staging_events_copy = ("""

    COPY staging_events FROM '{}'
    iam_role '{}'
    region 'us-west-2'
    FORMAT as JSON '{}'
    TIMEFORMAT as 'epochmillisecs'
""").format(LOG_DATA,DWH_ROLE_ARN,LOG_JSONPATH)

staging_songs_copy = ("""
 COPY staging_songs FROM '{}'
    iam_role '{}'
    region 'us-west-2'
    FORMAT as JSON 'auto'
""").format(SONG_DATA,DWH_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT
    e.ts as start_time
    ,e.userid as user_id
    ,e.level
    ,s.song_id
    ,s.artist_id
    ,e.sessionid as session_id
    ,e.location
    ,e.useragent
FROM staging_events e, staging_songs s
    WHERE
    1=1
    and s.title = e.song 
    and e.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userid as user_id
    ,firstname as first_name
    ,lastname as last_name
    ,gender
    ,level 
    FROM staging_events
    WHERE 
    page = 'NextSong' AND
    user_id is not null
""")

song_table_insert = ("""
INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id
    ,title
    ,artist_id
    ,year
    ,duration
    FROM staging_songs
    WHERE song_id is not null
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude,longitude)
SELECT DISTINCT
    artist_id
    ,artist_name as name
    ,artist_location as location
    ,artist_latitude as latitude
    ,artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id is not NULL
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT 
    ts as start_time
    ,extract(hour from ts) as hour
    ,extract(day from ts) as day
    ,extract(week from ts) as week
    ,extract(month from ts) as month
    ,extract(year from ts) as year
    ,extract(weekday from ts) as weekday
    FROM staging_events
    WHERE ts is not null
""")

# QUERY LISTS

# table_order should follow this order to create due to references [staging_events_table, staging_songs_tabl, user_table,artist_table,  song_table, time_table, songplay_table]
# table_order should follow this order to drop due to references [songplay_table,staging_events_table, staging_songs_table, user_table, song_table,artist_table,   time_table]

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create,song_table_create,time_table_create,songplay_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, artist_table_drop,song_table_drop,time_table_drop,songplay_table_drop]
drop_table_queries = [songplay_table_drop,staging_events_table_drop, staging_songs_table_drop, user_table_drop,song_table_drop, artist_table_drop,time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
