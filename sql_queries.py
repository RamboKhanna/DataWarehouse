import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                               CREATE TABLE IF NOT EXISTS stagingevents(
                               artist              VARCHAR,
                               auth                VARCHAR,
                               first_name          VARCHAR,
                               gender              VARCHAR,
                               iteminsession       INT,
                               lastname            VARCHAR,
                               length              DECIMAL,
                               level               VARCHAR,
                               location            VARCHAR,
                               method              VARCHAR,
                               page                VARCHAR,
                               registration        FLOAT8,
                               session_id          INT,
                               song                VARCHAR,
                               status              INT,
                               start_time          BIGINT,
                               user_agent          VARCHAR,
                               user_id             VARCHAR
                               );

""")

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS stagingsongs(
                                song_id            VARCHAR,
                                title              VARCHAR,
                                duration           DECIMAL,
                                year               INT,
                                artist_id          VARCHAR,
                                artist_name        VARCHAR,
                                latitude           DOUBLE PRECISION,
                                longitude          DOUBLE PRECISION,
                                location           VARCHAR,
                                num_songs          INT
                                );
""")

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id            INT IDENTITY(1,1) PRIMARY KEY, 
                            start_time             TIMESTAMP NOT NULL SORTKEY,
                            user_id                VARCHAR NOT NULL DISTKEY, 
                            level                  VARCHAR, 
                            song_id                VARCHAR, 
                            artist_id              VARCHAR, 
                            session_id             INT, 
                            location               VARCHAR, 
                            user_agent             VARCHAR
                            )DISTSTYLE KEY;
""")

user_table_create = ("""
                            CREATE TABLE IF NOT EXISTS users(
                            user_id                VARCHAR PRIMARY KEY, 
                            first_name             VARCHAR, 
                            lastname               VARCHAR,
                            gender                 VARCHAR,
                            level                  VARCHAR
                            )DISTSTYLE ALL;
""")

song_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songs(
                            song_id                VARCHAR PRIMARY KEY SORTKEY, 
                            title                  VARCHAR, 
                            artist_id              VARCHAR DISTKEY,
                            year                   INT, 
                            duration               DECIMAL
                            )DISTSTYLE KEY;
""")

artist_table_create = ("""
                            CREATE TABLE IF NOT EXISTS artists(
                            artist_id              VARCHAR PRIMARY KEY SORTKEY, 
                            name                   VARCHAR, 
                            location               VARCHAR,
                            latitude               DOUBLE PRECISION, 
                            longitude              DOUBLE PRECISION
                            )DISTSTYLE ALL;
""")

time_table_create = ("""
                            CREATE TABLE IF NOT EXISTS time(
                            start_time             TIMESTAMP PRIMARY KEY SORTKEY, 
                            hour                   INT, 
                            day                    INT,
                            week                   INT, 
                            month                  INT, 
                            year                   INT DISTKEY, 
                            weekday                INT
                            )DISTSTYLE KEY;
""")

# STAGING TABLES

staging_events_copy = ("""
                            COPY {} FROM {}
                            IAM_ROLE {}
                            JSON {} region '{}';
                       """).format('stagingevents',
                                    config['S3']['LOG_DATA'],
                                    config['IAM_ROLE']['ARN'],
                                    config['S3']['LOG_JSONPATH'],
                                    config['CLUSTER']['REGION']
                                  )

staging_songs_copy = ("""
                            COPY {} FROM {}
                            IAM_ROLE {}
                            JSON 'auto' region '{}';
                      """).format('stagingsongs',
                                   config['S3']['SONG_DATA'],
                                   config['IAM_ROLE']['ARN'],
                                   config['CLUSTER']['REGION']
                                  )

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT TIMESTAMP 'epoch' + (e.start_time/1000 * INTERVAL '1 second'),
                                    e.user_id,
                                    e.level,
                                    s.song_id,
                                    s.artist_id,
                                    e.session_id,
                                    e.location,
                                    e.user_agent
                            FROM stagingevents e
                            JOIN stagingsongs s ON (e.song=s.title) AND (e.artist=s.artist_name) AND ABS(e.length-s.duration)<2 
                            WHERE e.page= 'NextSong'
""")

user_table_insert = ("""
                            INSERT INTO users SELECT DISTINCT (user_id)
                                    user_id,
                                    first_name,
                                    lastname,
                                    gender,
                                    level
                            FROM stagingevents
""")

song_table_insert = ("""
                            INSERT INTO songs SELECT DISTINCT (song_id) 
                                    song_id,
                                    title,
                                    artist_id,
                                    year,
                                    duration
                            FROM stagingsongs
""")

artist_table_insert = ("""
                            INSERT INTO artists SELECT DISTINCT (artist_id)
                                    artist_id,
                                    artist_name,
                                    location,
                                    latitude,
                                    longitude
                            FROM stagingsongs
""")


time_table_insert = ("""
                            INSERT INTO time
                            WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (start_time/1000 * INTERVAL '1 second') AS ts 
                            FROM stagingevents)
                            SELECT DISTINCT  ts,
                            EXTRACT(HOUR FROM ts),
                            EXTRACT(DAY FROM ts),
                            EXTRACT(WEEK FROM ts),
                            EXTRACT(MONTH from ts),
                            EXTRACT(YEAR FROM ts),
                            EXTRACT(weekday from ts)
                            FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


