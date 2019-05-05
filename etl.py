import psycopg2
import pandas as pd
import sql_queries as sql
from utils import get_timestamp_attrs, get_files_in_dir
import ipdb

SONG_FIELDS = ['song_id', 'title', 'artist_id', 'year', 'duration']
ARTIST_FIELDS = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
TIME_FIELDS = ['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
USER_FIELDS = ['userId', 'firstName', 'lastName', 'gender', 'level']
SONG_DATA_DIR = 'data/song_data'
LOG_DATA_DIR = 'data/log_data'


def insert_songs(cur, df):
    """Insert song data into songs table"""
    song_data = (df
                 .filter(SONG_FIELDS)
                 .values
                 .tolist())

    cur.executemany(sql.song_table_insert, song_data)


def insert_artists(cur, df):
    """Insert artist data into artists table"""
    artist_data = (df
                   .filter(ARTIST_FIELDS)
                   .values
                   .tolist())

    cur.executemany(sql.artist_table_insert, artist_data)


def insert_time(cur, df):
    """Insert time data into time table"""
    t = pd.to_datetime(df['ts'], unit='ms')
    time_data = pd.DataFrame([get_timestamp_attrs(ts) for ts in t],
                             columns=TIME_FIELDS).values.tolist()

    cur.executemany(sql.time_table_insert, time_data)


def insert_user(cur, df):
    """Insert user data into user table"""
    user_data = (df
                 .filter(USER_FIELDS)
                 .values
                 .tolist())

    cur.executemany(sql.user_table_insert, user_data)


def insert_songplay(cur, df):
    """Insert songplay data into songplay table"""
    for index, row in df.iterrows():
        cur.execute(sql.song_select, {
            'title': row.song,
            'duration': row.length,
            'artist_name': row.artist
        })

        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = [row.ts / 1000, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent]

        cur.execute(sql.songplay_table_insert, songplay_data)


def process_song_file(cur, dirpath):
    """Process song file to process song and artist data"""
    filepaths = get_files_in_dir('data/song_data')
    df = pd.concat([pd.read_json(f, lines=True) for f in filepaths],
                   ignore_index=True)

    insert_songs(cur, df)
    insert_artists(cur, df)


def process_log_file(cur, dirpath):
    """Process log file to process time, user and songplay data"""
    filepaths = get_files_in_dir('data/log_data')
    df = pd.concat([pd.read_json(f, lines=True) for f in filepaths],
                   ignore_index=True)

    df = df[df['page'] == 'NextSong']

    insert_time(cur, df)
    insert_user(cur, df)
    insert_songplay(cur, df)


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=Textbook13!")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_song_file(cur, SONG_DATA_DIR)
    process_log_file(cur, LOG_DATA_DIR)

    conn.close()


if __name__ == "__main__":
    main()
