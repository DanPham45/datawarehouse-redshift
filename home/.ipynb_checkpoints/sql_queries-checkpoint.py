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
CREATE TABLE
    IF NOT EXISTS staging_events (
    event_id INT IDENTITY(0,1),
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR(1),
    item_in_session INT,
    last_name VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INT,
    PRIMARY KEY(event_id)
    )
    --DISTKEY(artist)
    --SORTKEY(song, page, ts)
""")

staging_songs_table_create = ("""
CREATE TABLE
    IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT(8),
    artist_longitude FLOAT(8),
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DOUBLE PRECISION,
    year INT,
    PRIMARY KEY(song_id)
    )
    DISTKEY(artist_name)
    SORTKEY(title)
""")

songplay_table_create = ("""
CREATE TABLE
	IF NOT EXISTS songplays (songplay_id INT IDENTITY(0,1),
	start_time TIMESTAMP NOT NULL,
	user_id INT NOT NULL,
	LEVEL VARCHAR,
	song_id VARCHAR,
	artist_id VARCHAR,
	session_id INT NOT NULL,
	location VARCHAR,
	user_agent VARCHAR,
    PRIMARY KEY(songplay_id)
    )
    DISTKEY(artist_id)
    SORTKEY(song_id)
""")

user_table_create = ("""
CREATE TABLE
	IF NOT EXISTS users (user_id INT,
	first_name VARCHAR,
	last_name VARCHAR,
	gender VARCHAR(1),
	LEVEL VARCHAR,
    PRIMARY KEY(user_id)
    )
    SORTKEY(user_id)
""")

song_table_create = ("""
CREATE TABLE
	IF NOT EXISTS songs (song_id VARCHAR,
	title VARCHAR,
	artist_id VARCHAR NOT NULL,
	YEAR INT,
	duration FLOAT,
    PRIMARY KEY(song_id)
    )
    SORTKEY(song_id)
""")

artist_table_create = ("""
CREATE TABLE
	IF NOT EXISTS artists (artist_id VARCHAR,
	name VARCHAR,
	location VARCHAR,
	latitude FLOAT(8),
	longitude FLOAT(8),
    PRIMARY KEY(artist_id)
    )
    DISTKEY(artist_id)
""")

time_table_create = ("""
CREATE TABLE
	IF NOT EXISTS TIME (start_time TIMESTAMP NOT NULL,
	HOUR INT,
	DAY INT,
	week INT,
	MONTH INT,
	YEAR INT,
	weekday INT
    )
    SORTKEY(start_time)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
json {} 
region 'us-west-2';
""").format(config.get('S3','LOG_DATA'), 
            config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
json 'auto' 
region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT
	INTO
		songplays(start_time,
		user_id,
		LEVEL,
		song_id,
		artist_id,
		session_id,
		location,
		user_agent)
	SELECT DISTINCT
    cast(timestamp 'epoch' + e.ts/1000 * interval '1 second' as timestamp),
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
    FROM staging_events e,
    staging_songs s
    WHERE s.artist_name = e.artist
    and s.title = e.song
    and s.duration = e.length
    and e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT
	INTO
		users(user_id,
		first_name,
		last_name,
		gender,
		LEVEL)
	SELECT DISTINCT user_id,
    first_name,
    last_name,
    gender,
    level
    FROM staging_events
    where page = 'NextSong'
""")

song_table_insert = ("""
INSERT
	INTO
		songs(song_id,
		title,
		artist_id,
		YEAR,
		duration)
	SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT
	INTO
		artists (artist_id,
		name,
		location,
		latitude,
		longitude)
    SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
INSERT
	INTO
		TIME (start_time,
		HOUR,
		DAY,
		week,
		MONTH,
		YEAR,
		weekday)
	SELECT DISTINCT
    start_time,
    EXTRACT(HOUR FROM start_time),
    EXTRACT(DAY FROM start_time),
    EXTRACT(WEEK FROM start_time),
    EXTRACT(MONTH FROM start_time),
    EXTRACT(YEAR FROM start_time),
    EXTRACT(WEEKDAY FROM start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, 
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, 
                        artist_table_insert, time_table_insert]
