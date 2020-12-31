import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (staging_event_id bigint IDENTITY(0,1) PRIMARY KEY, artist varchar, auth varchar(30), first_name varchar, \
    gender varchar(8), item_in_session int, last_name varchar, length numeric, level varchar, location varchar, method varchar, page varchar, registration bigint, \
    session_id int, song varchar, status int, ts TIMESTAMP, user_agent varchar, user_id int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (staging_song_id bigint IDENTITY(0,1) PRIMARY KEY, num_songs int, artist_id varchar(30), artist_latitude numeric, \
    artist_longitude numeric, artist_location varchar, artist_name varchar, song_id varchar(30), title varchar, duration numeric, year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint IDENTITY(0,1) PRIMARY KEY, start_time TIMESTAMP NOT NULL, user_id int NOT NULL, level varchar, song_id varchar, \
    artist_id varchar, session_id int NOT NULL, location varchar, user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY sortkey, first_name varchar, last_name varchar, gender varchar, level varchar) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY sortkey, title varchar, artist_id varchar, year int, duration float);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY sortkey, name varchar, location varchar, latitute float, longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY sortkey, hour int, day int, week int, month int, year int, weekday int);
""")

# STAGING TABLES

staging_events_copy = (f"""
    copy staging_events from {config.get('S3','LOG_DATA')}
    credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
    JSON {config.get('S3','LOG_JSONPATH')}
    TIMEFORMAT 'epochmillisecs'
    compupdate off region 'us-west-2';
""")

staging_songs_copy = (f"""
    copy staging_songs from {config.get('S3','SONG_DATA')}
    credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
    JSON 'auto' 
    compupdate off region 'us-west-2';
""")


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts, e.user_id, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
FROM staging_events e LEFT JOIN staging_songs s
ON e.song = s.title AND e.length = s.duration AND e.artist = s.artist_name
WHERE page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT user_id, first_name, last_name, gender, level FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id , title, artist_id, year, duration FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitute, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts) from staging_events
WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
