import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process songs files and insert records into the Postgres database.
    Parameters:
        cur (cursor): psycopg2 cursor connection to postgres
        filepath (string): song file path 
    """
#   open song file

    df = pd.read_json(filepath, lines=True, orient='columns')
    
#   insert song record                   
    song_data = (df.values[0][7], df.values[0][8], df.values[0][0], df.values[0][9], df.values[0][5])
    cur.execute(song_table_insert, song_data)
    
#   insert artist record
    artist_data = (df.values[0][0], df.values[0][4], df.values[0][2], df.values[0][1], df.values[0][3])
    cur.execute(artist_table_insert, artist_data)
    
def process_log_file(cur, filepath):
    """
    Function to process log files and store data into time, users and songplay
    tables
    Parameters:
        cur (cursor): psycopg2 cursor connection to postgres
        filepath (string): log file path
    """   
    
#   open log file
    
    df = pd.DataFrame(pd.read_json(filepath, lines=True, orient='columns'))
    df_orig = df

#   filter by NextSong action
    
    df = df[df['page']=='NextSong']
                                                                                       
#   convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
#   insert time data records
                    
    time_data = list(zip(t.dt.strftime('%Y-%m-%d %I:%M:%S'), t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

#   load user table
    user_data = df_orig.get(['userId', 'firstName', 'lastName', 'gender', 'level'])

#   column names
    user_data.columns= ['user_id', 'first_name', 'last_name', 'gender', 'level']

#   remove duplicates
    user_data_duplicates_removed= user_data.drop_duplicates('user_id', keep='first')
    user_df = user_data_duplicates_removed
    
#   insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

#   insert songplay records
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone() 
        
        if results:
            songid, artistid = results
            print(songid, artistid)
        else:
            songid, artistid = None, None
            
#       insert songplay record
        start_time = pd.to_datetime(row.ts, unit='ms').strftime('%Y-%m-%d %I:%M:%S')
        songplay_data = (start_time, row.userId, row.level, str(songid), str(artistid), row.sessionId, row.location, row.userAgent)
#       print(songplay_data)
        cur.execute(songplay_table_insert, songplay_data)
        if songid:
            print (songplay_data)
            print('Record inserted')
        

        
def process_data(cur, conn, filepath, func):
    """
    Function to process all the data set files and store data into the database
    Parameters:
        cur: psycopg2 cursor
        conn: psycopg2 connection object
        filepath (string): path where the data set files are located
        func (function): function to execute (process_song_file or
        process_log_file)
    """
#   get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

#   get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

#   iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
    print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    #print("Song table after insert")
    #cur.execute("select * from songs")
    #row=cur.fetchone()
    #while row:
    #   print(row)
    #   row = cur.fetchone()
    
    conn.close()
    
if __name__ == "__main__":
    main()