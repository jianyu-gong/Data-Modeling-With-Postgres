import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function is used to read the song files and extract the song and artist information then insert into song
    and artist tables.
    :param cur: cursor; sparkifydb
    :param filepath: str; the location of song file
    :return: none
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [df.song_id.values.tolist()[0], df.title.values.tolist()[0],
                 df.artist_id.values.tolist()[0], df.year.values.tolist()[0], df.duration.values.tolist()[0]]
    print(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.artist_id.values.tolist()[0], df.artist_name.values.tolist()[0],
                   df.artist_location.values.tolist()[0], df.artist_latitude.values.tolist()[0],
                   df.artist_longitude.values.tolist()[0]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function is used to read the log files and extract the user activities information then insert into time, user
    and song played tables.
    :param cur: cursor; sparkifydb
    :param filepath: str; the location of log file
    :return: none
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame.from_dict({"start_time": t,
                                      "hour": t.dt.hour,
                                      "day": t.dt.day,
                                      "week": t.dt.dayofweek,
                                      "month": t.dt.month,
                                      "year": t.dt.year,
                                      "weekday": t.dt.weekday})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame.from_dict({"user_id": df['userId'],
                                      "first_name": df['firstName'],
                                      "last_name": df['lastName'],
                                      "gender": df['gender'],
                                      "level": df['level']})

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'),
                         row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function is used to iterate all the files under filepath folder and process using a specified function.
    :param cur: cursor
    :param conn: connect; connect to specified database
    :param filepath: str; location of the data file
    :param func: function; the function used to process the data
    :return: none
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
