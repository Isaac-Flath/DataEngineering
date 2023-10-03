import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

fpaths = config['S3']
iam_arn = config['IAM_ROLE']['ARN']


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs "
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


fpaths['LOG_DATA']
fpaths['LOG_JSONPATH']
iam_arn
staging_events_copy = """
COPY staging_events
FROM {fpath_log}
iam_role {arn}
region 'us-east-1'
FORMAT AS JSON {fpath_json}
timeformat as 'epochmillisecs'
""".format(fpath_log=fpaths['LOG_DATA'],
        fpath_json=fpaths['LOG_JSONPATH'],
        arn=iam_arn
    )


fpaths['SONG_DATA']
iam_arn
staging_songs_copy = """
COPY staging_songs
FROM {fpath}
region 'us-east-1'
iam_role {arn}
FORMAT AS JSON 'auto';
""".format(fpath=fpaths['SONG_DATA'],
        arn=iam_arn)

staging_events_table_create = """
CREATE TABLE staging_events (
    artist              VARCHAR(125),
    auth                VARCHAR(10),
    first_name          VARCHAR(10),
    gender              VARCHAR(1),
    item_in_session     INT,
    last_name           VARCHAR(10),
    length              DECIMAL,
    level               VARCHAR(4),
    location            VARCHAR(50),
    method              VARCHAR(3),
    page                VARCHAR(20),
    registration        BIGINT,
    sessions_id         INT,
    song                VARCHAR(200),
    status              INT,
    ts                  TIMESTAMP,
    user_agent          VARCHAR(150),
    user_id             INT
    );
"""

staging_songs_table_create = """
CREATE TABLE staging_songs (
    num_songs           INT,
    artist_id           VARCHAR(18),
    artist_latitude     DECIMAL,
    artist_longitude    DECIMAL,
    artist_location     VARCHAR(300),
    artist_name         VARCHAR(500),
    song_id             VARCHAR(18),
    title               VARCHAR(300),
    duration            DECIMAL,
    year                INT
    );
""" 

songplay_table_create = """
CREATE TABLE songplays ( 
    songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
    start_time          TIMESTAMP, 
    user_id             INT, 
    level               VARCHAR, 
    song_id             VARCHAR, 
    artist_id           VARCHAR, 
    session_id          INT, 
    location            VARCHAR, 
    user_agent          VARCHAR
    )
DISTSTYLE EVEN;
"""

user_table_create = """
CREATE TABLE users ( 
    user_id             INT             PRIMARY KEY SORTKEY, 
    first_name          VARCHAR, 
    last_name           VARCHAR,
    gender              VARCHAR, 
    level               VARCHAR
    )
DISTSTYLE ALL;
"""

song_table_create = """
CREATE TABLE songs ( 
    song_id             VARCHAR(18)         PRIMARY KEY SORTKEY, 
    title               VARCHAR(300),
    artist_id           VARCHAR(18), 
    year                INT,
    duration            DECIMAL
    )
DISTSTYLE ALL;

"""

artist_table_create =  """
CREATE TABLE artists ( 
    artist_id           VARCHAR         PRIMARY KEY SORTKEY,
    name                VARCHAR(500),
    location            VARCHAR(300), 
    latitude            DECIMAL, 
    longitude           DECIMAL
    )
DISTSTYLE ALL;
"""

time_table_create = """
CREATE TABLE time ( 
    start_time          TIMESTAMP       PRIMARY KEY SORTKEY, 
    hour                INT,  
    day                 INT, 
    week                INT, 
    month               INT, 
    year                INT, 
    weekday             VARCHAR(20)
    )
DISTSTYLE ALL;
""" 

songplay_table_insert = """
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
SELECT
    distinct(e.ts) as start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessions_id,
    e.location,
    e.user_agent
FROM staging_events e
JOIN staging_songs s ON 
    e.song = s.title AND 
    e.artist = s.artist_name AND 
    e.page  =  'NextSong'
"""

user_table_insert = """
INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
    )
SELECT 
    distinct(user_id),
    first_name,
    last_name,
    gender,
    level
FROM staging_events
WHERE user_id is not null
"""

song_table_insert = """
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
    )
SELECT 
    distinct(song_id),
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id is not null
"""

artist_table_insert = """
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT
    distinct(artist_id),
    artist_name as name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
where artist_id is not null
"""

time_table_insert = """
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
select
    distinct(start_time) as start_time,
    EXTRACT(hour FROM start_time)       AS hour,
    EXTRACT(day FROM start_time)        AS day,
    EXTRACT(week FROM start_time)       AS week,
    EXTRACT(month FROM start_time)      AS month,
    EXTRACT(year FROM start_time)       AS year,
    EXTRACT(dayofweek FROM start_time)  as weekday
FROM songplays
"""


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, 
        songplay_table_create, user_table_create, song_table_create, 
        artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
        songplay_table_drop, user_table_drop, song_table_drop, 
        artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
