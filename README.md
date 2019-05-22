#  Data Modeling with Postgres & ETL Pipeline
***
## Introduction

Sparkify is a music streaming start-up company. The have collected many songs and user activities data through their 
music app. And the analytics team is particularly interested in understanding what songs users are listening to. 
Currently, their data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON 
metadata on the songs in their app. However, this cannot provid an easy way to query the data. 

## The Goal
***
The goal is to create a database schema and ETL pipeline for this analysis. The database and ETL pipeline will be tested
by running queries given by the analytics team from Sparkify and compare results with their expected results.

## Database & ETL Pipeline
***
Using the song and log datasets, a star schema is created including one fact table: **songplays**, and four dimension 
tables: **users**, **songs**, **artists** and **time**.

## Exmaple Queries
***
* What are the top 10 most popular songs in 2018? 

```SQL
  SELECT COUNT(a.song_id) as 'number_played', a.title
  (SELECT * 
  FROM songplays JOIN songs ON songplays.song_id = songs.song_id) a
  GROUP BY a.song_id
  ORDER BY number_played DESC
```

## Code Explanations
**create_tables.py:** drops and creates your tables. Run this file to reset your tables before each time you run 
your ETL scripts.

**elt.py:** reads and processes files from song_data and log_data and loads them into tables. 

**sql_queries.py:** contains all your sql queries, and is imported into the last three files above

**test.ipynb** displays the first few rows of each table to let you check your database.

**etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into your tables. 
This notebook contains detailed instructions on the ETL process for each of the tables.