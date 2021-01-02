# Sparkify 
The objective of this project is to 
* do data modelling with Postgres
* build an ETL pipeline using Python
* define fact and dimension tables for a star schema to help focus on analytics
* write ETL pipeline that transfers data from files into tables in Postgres using Python and SQL

## Project Description
A startup called Sparkify wants to analyze the data
they've been collecting on songs and user activity on their new music streaming
app. The analytics team is particularly interested in understanding what songs
users are listening to. Currently, they don't have an easy way to query their
data, which resides in a directory of JSON logs on user activity on the app, as
well as a directory with JSON metadata on the songs in their app.

### Datasets
The data is collected for user activities and songs played in two directories using JSON files:
We are focussiong on the data collected and stored in two datatasets: 
* `song_data`
* `log_data`


#### Songs dataset
The Songs dataset is a subset of real data from Million SOng Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song.

Below is a sample of a file stored on the path `data/song_data` in JSON format:
```
{
    "num_songs": 1,
    "artist_id": "ARD7TVE1187B99BFB1",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "California - LA",
    "artist_name": "Casual",
    "song_id": "SOMZWCG12A8C13C480",
    "title": "I Didn't Mean To",
    "duration": 218.93179,
    "year": 0
}

```
#### Logs dataset

The Log dataset consists of log files in JSON format based on the activity logs created by event simulator.

Below is a sample of a file stored on the path `data/log_data` in JSON format:
```
{
    "artist": null,
    "auth": "Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET",
    "page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\"",
    "userId": "39"
}
```

### Database schema
Using the song and log dataset described above, we will have to create star schema which is optimized for 
queries that can do analysis of the songs played.
#### Fact Table 

**songplays** - With pages' 'Next song, it records in log data associated with song plays 

#### Table Songplays
```sql
songplays (
	songplay_id SERIAL PRIMARY KEY,
	start_time timestamp NOT NULL,
	user_id varchar NOT NULL,
	level varchar NULL,
	song_id varchar NOT NULL,
	artist_id varchar NOT NULL,
	session_id int NOT NULL,
	location varchar NULL,
	user_agent varchar NULL
);
```
#### Dimension Table
**users**  - users in the Sparkify app
#### Table Users
```sql
users (
    user_id varchar PRIMARY KEY UNIQUE NOT NULL,
    first_name varchar NULL,
    last_name varchar NULL,
    gender varchar NULL,
    level varchar NULL
);
```
**songs**  - songs in music database
#### Table Songs
```sql
songs (
    song_id PRIMARY KEY UNIQUE NOT NULL,
    title varchar NULL,
    artist_id varchar NULL,
    year int NULL,
    duration numeric NULL
);

```
**artists**  - artists in music database
### Table artists
```sql
artists (
	artist_id varchar PRIMARY KEY UNIQUE NOT NULL,
	name varchar NULL,
	location varchar NULL,
	latitude decimal NULL,
	longitude decimal NULL
);
```
**time**  - timestamps of records in  **songplays**  broken down into specific units
#### Table Time
```sql
time (
    
    start_time timestamp NOT NULL,
    hour int NULL,
    day int NULL,
    week int NULL,
    month int NULL,
    year int NULL,
    weekday int NULL
);

```
### Environment 
Python 3.6 or above

PostgresSQL 9.5 or above

psycopg2 - PostgreSQL database adapter for Python

### Project Files

```sql_queries.py``` : contains sql queries for 

* creating fact and dimension tables
* dropping tables 
* insert.

```create_tables.py``` : contains code for 

* setting up database - to create **sparkifydb
* creating the fact table
* creating dimension tables

```etl.ipynb``` : a jupyter notebook that is used to 

* run test ETL queries
* to analyse dataset before loading

```etl.py``` : contains code to

* read  **song_data and **log_data
* process **song_data and **log_data
* establish the ETL pipeline

```test.ipynb``` : a jupyter notebook that is used to 

* connect to postgres db 
* validate the data loaded

### Process
#### Database
To create the database and tables we run the script `create_tables.py` that will import all the queries stored in `sql_queries` and will execute them.

First it will drop all existing tables and the database, then it will create the database and all tables.

Each table has an index (Primar Key) which will be assigned from the data obtained by the ETL process. 

WE write into tables using the clause `ON CONFLICT DO NOTHING` on all the insert queries to avoid the scripts from raising errors on conflict.

#### ETL
The script `etl.py` provides all the functions to perform the ETL process.

The function `process_song_file` will read each .JSON file located in the `data/song_data` folder to extract data and write into the `artist` and `songs` table

The function`get_flies` gets a list of all the log .JSON files located in the `data/log_data` folder
By parsing each log file,we create the time and users dimensional tables and the songplays fact table.

For extrating time, each record is filtered by `NextSong` action and Extract the timestamp, hour, day, 
week of year, month, year, and weekday from the ts column and set time_data to a list containing these 
values in order by converting the ts timestamp column to datetime

the following methods are used to filter the records and decode the timestamp value:

```python
    df = df[df.page == "NextSong"] 

    t = pd.to_datetime(df['ts'], unit='ms')
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year,t.dt.weekday) 
```

To get the data for the `users` table, the script will 
* read 'userId','firstName','lastName', 'gender', 'level', 
* remove duplicates 
* and insert unique user records into the `users` table.

To get data fpr tje `songplays` table, the script will
* extract `song_id` and `artist_id` from the `log` .JSON file
* decode `ts` and extract the datetime value
* write the data into `songplay` table

## Project Instructions
### Create database and tables

- Run `python3 create_tables.py` to create the database and tables.
- Run `test.ipynb` Juppyter notebook to confirm the creation of the tables with the correct columns.

### ETL Process
After creating the database and tables we can start the ETL process:
- Run `python3 etl.py` 
This will extract data from all the .json files located at the `data` folder and store it on the Postgres database. 
