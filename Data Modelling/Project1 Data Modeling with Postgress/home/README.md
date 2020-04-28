## Project: Data Modeling with Postgres

### Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. 
### Project Description
In this project, I will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

### Python script files

The script will connect to the Sparkify database, drops any tables if they exist, and creates the tables.
```sh
python create_tables.py
```

The script will connect to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables.
```sh
python etl.py
```

### Jupyter Notebook
`etl.ipynb` notebook to delveop ETL process for each table. `test.ipynb` to confirm that records were succesfully inserted into each table.


### Schema Design
Using the song and log dataset, star schema will be created with below tables
##### Fact Table
   1.  **songplays**  - records in log data associated with song plays i.e. records with page 
        * *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

##### Dimension table 

2. **users** - users in the app
    * *user_id, first_name, last_name, gender, level*
3. **songs** - songs in music database
    * *song_id, title, artist_id, year, duration*
4. **artists** - artists in music database
    * *artist_id, name, location, latitude, longitude*
5. **time** - timestamps of records in songplays broken down into specific units
   * *start_time, hour, day, week, month, year, weekda*

#### The project files
In addition to the data files, the project workspace includes five files:


1. `test.ipynb` displays the first few rows of each table to let you check your database.
2. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. `sql_queries.py` contains all your sql queries, and is imported into the last three files above.





