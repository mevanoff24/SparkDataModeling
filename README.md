# Background Information 

Sparkify has been growing rapidly and Sparkify now needs to move their data warehouse to a data lake. The analytics team is particularly interested in understanding what songs users are listening to. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This will allow their analytics team to continue finding insights in what songs their users are listening to.



# Solution 

I have created a Spark ETL pipeline that 
- extracts their CSV and JSON data from S3
- process the song and log files
- uploaded the data back into S3 as a set of dimensional tables ([parquet files](https://parquet.apache.org/)). 

Now the their analytics team can easily develop insights in what songs their users are listening to andunderstand user behavior.


## How to run 
- Update the config file (`dl.cfg`) with your AWS credentails and access key
- Run `python etl.py` from this root directory (command line arguments coming soon)


## Dependencies
- `pyspark`
- `datetime`
- `configparser`
- `prettytable` (for sample queries)



## Additional Design Info 

### Fact and Dimension Tables

Facts and dimensions form the core of any business intelligence effort. These tables contain the basic data used to conduct detailed analyses and derive business value. 
- A fact table consists of the measurements, metrics or facts of a business process.  Events that have actually happened. 
- A dimension table is a structure that categorizes facts and measures in order to enable users to answer business questions. Commonly used dimensions are people, products, place and time.

### Star Schema
Here I have developed a star schema for Sparkify. The star schema separates business process data into facts, which hold the measurable, quantitative data about a business, and dimensions which are descriptive attributes related to fact data. A star schema allows for simpler queries, fast aggregations and the ability to denormalize your data. 


### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
**users** - users in the app
- user_id, first_name, last_name, gender, level

**songs** - songs in music database
- song_id, title, artist_id, year, duration

**artists** - artists in music database
- artist_id, name, location, lattitude, longitude

**time** - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday



## Example Queries

After seeting up your spark context (named `spark`), run the queries of 

### Query 1

Find me the percentage of 'paid' accounts for males and females 

```
user_df = spark.read.parquet('user_data.parquet')
user_df.registerTempTable("user_data")

gender_paid_counts = spark.sql(
    """
    SELECT 
        COUNT(*) GENDER_COUNT,
        gender, 
        ROUND(AVG(CASE WHEN level = 'paid' THEN 1 ELSE 0 END), 2) PAID_PERCENT
    FROM user_data
    GROUP BY 2
    """).collect()
t = PrettyTable(['GENDER', 'GENDER_COUNT', 'PAID_PERCENT'])
for row in gender_paid_counts:
    t.add_row([row.gender, row.GENDER_COUNT, row.PAID_PERCENT])
print(t)
```
```
+--------+--------------+--------------+
| GENDER | GENDER_COUNT | PAID_PERCENT |
+--------+--------------+--------------+
|   F    |      60      |     0.25     |
|   M    |      44      |     0.16     |
+--------+--------------+--------------+
```

### Query 2

Find me the top 5 artists with the longest average song length
```
song_df = spark.read.parquet('song_data.parquet')
song_df.registerTempTable("song_data")

longest_average_song_length_per_artist = spark.sql(
    """
    SELECT 
        ROUND(AVG(duration), 2) AVERAGE_SONG_LENGTH, 
        artist_name 
    FROM song_data
    WHERE year > 0
    GROUP BY 2
    ORDER BY 1 DESC
    LIMIT 5
    """).collect()
t = PrettyTable(['ARTIST NAME', 'AVERAGE_SONG_LENGTH'])
for row in longest_average_song_length_per_artist:
    t.add_row([row.artist_name, row.AVERAGE_SONG_LENGTH])
print(t)
```
```
+----------------+---------------------+
|  ARTIST NAME   | AVERAGE_SONG_LENGTH |
+----------------+---------------------+
|   Blue Rodeo   |        491.13       |
|  Steve Morse   |        363.86       |
| Terry Callier  |        342.57       |
| Chase & Status |        337.68       |
| Sierra Maestra |        313.13       |
+----------------+---------------------+
```


