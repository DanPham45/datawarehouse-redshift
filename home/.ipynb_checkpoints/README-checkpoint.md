# Project Summary

This project is to create a database schema and ETL pipeline for an analysis that focuses on analyzing the songs users listening to.

## Usage

Open commandline and run the following script to create tables.
```bash
python create_tables.py
```
When that is done, run the following script to fetch and insert data from S3 into the predefined tables.
```bash
python etl.py
```
## Other files in the repository
There are 6 files in the workspace:

* create_tables.py: resets and creates tables before performing ETL
* etl.py: performs ETL job by reading and processing files from S3 and loading them into tables
* sql_queries.py: defines all sql queries. This file is referenced inside the 2 files above
* README.md: explanation on the project
 
 ## Database design
 There is one database created in this project called "sparkifydb". It includes 4 dimension tables and 1 fact tables:
     
* **songplays** (fact table) - records in log data associated with song plays i.e. records with page NextSong. Columns are: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* **users** - users in the app. Columns are: user_id, first_name, last_name, gender, level
* **songs** - songs in the database. Columns are: song_id, title, artist_id, year, duration
* **artists** - artists in the database. Columns are: artist_id, name, location, latitude, longitude
* **time** - timestamps of records in songplays broken down into specific units. Columns are: start_time, hour, day, week, month, year, weekday

 ## ETL Process
After the database and tables were created, the ETL process can start. It would go through 2 separate processes:
* load_staging_tables: reads data from S3, inserts data into staging_songs and staging_events
* insert_tables: transforms and inserts data into artists, songs, time, users and songplays tables
 