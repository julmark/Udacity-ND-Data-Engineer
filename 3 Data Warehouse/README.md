# Project: Data Warehouse

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. They want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal of this project is to build an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the Sparkify's analytics team to continue finding insights.

## Table of Contents
* [Getting Started](#Getting-Started)
* [Setup and Run](#Setup-and-Run)

## Getting Started

For this project we are using two data sets - Song and Log Dataset - that both reside in S3.

### Available Data

Both datasets consists of files in JSON format. The Song Dataset contains metadata about a song and the artist of this song.

num_songs | artist_id | artisti_latitude | artist_longitude | artist_location | artist_name | song_id | title | duration | year
--- | --- | --- | --- |--- |--- |--- |--- |--- |--- 
1 | AR8IEZO1187B99055E | null | null | Panama | Danilo Perez | SOGDBUF12A8C140FAA | City Slickers | 149.86404 | 2008 

The Log Dataset consists of activity logs from a music streaming app.

artist | auth | firstName | gender | itemInSession | lastName | length | level | location | method | page | registration | sessionId | song | ...
--- | --- | --- | --- |--- |--- |--- |--- |--- |--- |--- |--- |--- |--- |---
Sophie B. Hawkins | Logged In | Katrin | F | 1 | Neruda | 99.1234 | free | New York | PUT | NextSong | 1542837407796 | 200 |  Harajuku Girls | ...


### Data Modell

Our data will be organized as a star schema within following tables:

**Fact Table**
1. *songplays* - records in log data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
2. *users* - users in the app
    * user_id, first_name, last_name, gender, level
3. *songs* - songs in music database
    * song_id, title, artist_id, year, duration
4. *artists* - artists in music database
    * artist_id, name, location, latitude, longitude
5. *time* - timestamps of records in songplays broken down into specific units
    * start_time, hour, day, week, month, year, weekday


### Project files

* `create_tables.py` drops and creates all the tables. It must be run before the etl script
* `sql_queries.py` contains all the sql queries (for dropping and creating tables and for inserting values)
* `etl.py` loads data from S3 into staging tables on Redshift and then processes that data into analytics tables on Redshift
* `README.md` provides information on this project


## Setup and Run

### Setup: Redshift Cluster

For this project we need a Redshift Cluster on Amazon Web Services (AWS). Make sure to edit the dwh.cfg file to be able to establish a connection to the corresponding cluster.

### Run: Project steps

You can follow the steps below to create and fill the tables:

1. Import and run ```create_tables.py``` to create your database and tables
2. Import and run ```etl.py``` to fill staging tables first and then to fill the analytics tables on Redshift

Now our schema is ready for song play analysis.
