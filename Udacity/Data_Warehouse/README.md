
# Sparkify ETL Pipeline and Database Design

Sparkify is a fast-growing music streaming startup focused on delivering an exceptional listening experience to its user base. As the company scales, data analytics become crucial for understanding user behavior and optimizing the service. This project aims to build an ETL pipeline that extracts raw data from Amazon S3, stages it in Redshift, and transforms it into a structured schema for efficient analytics. Ideal for software engineers, this documentation provides details on the database design, its purpose, and offers example queries for in-depth music streaming analysis.

## Table of Contents
1. [Usage Instructions](#usage-instructions)
2. [Reason and Justification for the Database Schema Design](#reason-and-justification-for-the-database-schema-design)
3. [Purpose of the Database in Context of Sparkify](#purpose-of-the-database-in-context-of-sparkify)
4. [Example Queries for Song Play Analysis](#example-queries-for-song-play-analysis)

---

## Usage Instructions

> Note: This repository only stores code for running ELT pipeline and not infrastructure information.  For infrastructure definition see [this repository](https://github.com/Isaac-Flath/Infrastructure/tree/main/data-eng)

### Dependencies

This project runs using python 3 and requires the `psycopg2` and `credstash` libraries.  You can install these requirements with the following command.

```bash
pip install -r requirements.txt
```

### Running the Pipeline

To run the full pipeline run the code below in terminal.

> WARNING:  `create_tables.py` will drop and recreate tables, so data will be lost when that is run.  To avoid data loss do nor run `create_tables.py` unless you plan on starting from scratch.

```bash
python create_tables.py
python etl.py
```

### Error handling

If you need to debug or change tables it is recommended to back up your tables first.  While redshift does snapshot tables, it is best not to rely on those.

If `etl.py` fails to run due to the copy statements, the top 25 most recent failures will be saved to `load_errors.csv` for convenient debugging.  This is the part of the pipeline *most* likely to fail when in production as it will likely fail if there are any changes in input data.

### Repo Organization

+ `common.py`:  Stores functions needed for both parts of the pipeline (such as db connection and query runing functions)
+ `dwh.cfg`:  Stores configuration information such as data source location and database target location
+ `sql_queries.py`:  All sql queries used in the pipeline are defined here.  There are four general types of queries:
    + Drop all tables 
    + Create all tables
    + Copy raw data from s3 to staging tables
    + Insert data from staging tables to fact and dimension tables
+ `create_tables.py`:  Runs drop and create queries from `sql_queries.py` 
+ `etl.py`: Runs copy and insert queries from `sql_queries.py` 

## Reason and Justification for the Database Schema Design

### Staging Tables

- **staging_events**: This table is used for staging event logs relating to user activity on the app. The column types are designed to be compatible with the types of data that Sparkify's JSON logs are likely to contain.
  
- **staging_songs**: Similarly, this table is used to stage song metadata. The column types and lengths are optimized to suit the JSON metadata about the songs.

### Dimensional Tables

- **users**: This table focuses on user-specific data. It uses `user_id` as the primary key and a sort key, facilitating faster queries that focus on user-level information.

- **songs**: This table contains information on songs. The `song_id` serves as the primary and sort key.

- **artists**: This table focuses on artist-level data. `artist_id` is used as the primary and sort key.

- **time**: This table holds time-related information. The `start_time` field is both the primary and sort key.

### Fact Table

- **songplays**: This table records instances of song plays. It has an auto-incrementing `songplay_id` as its primary key. 

### Distribution Styles

- The fact table uses `DISTSTYLE EVEN` to distribute its rows evenly across all slices, ensuring good parallelism during queries.
  
- Dimensional tables use `DISTSTYLE ALL`, which copies the entire table to each node, reducing the need for data shuffling during joins.

The schema is designed to be normalized to eliminate redundancy and improve data integrity. Meanwhile, it is optimized to address the specific analytical queries that Sparkify is interested in, like song play analysis based on user demographics or time.

---

## Purpose of the Database in Context of Sparkify

As Sparkify scales its operations, a robust data architecture is critical for sustaining growth and improving the user experience. This ETL pipeline and database serve as the foundation for their analytics operations. 

- **User Experience**: Understanding song popularity, user behavior, and activity trends can help in curating a better music selection and user interface.

- **Business Strategy**: The analytical capabilities can provide insights into user engagement levels, preferred subscription levels (e.g., free vs. premium), and geographical hotspots, which are vital for business decisions.

- **Revenue Optimization**: The analysis could guide targeted promotions and advertising, thus driving revenue.

---

## Example Queries for Song Play Analysis

### Most Played Song

```sql
SELECT song_id, COUNT(*) as play_count
FROM songplays
GROUP BY song_id
ORDER BY play_count DESC
LIMIT 1;
```
### Highest Usage Time of Day by Hour

```sql
SELECT t.hour, COUNT(*) as play_count
FROM songplays s
JOIN time t ON s.start_time = t.start_time
GROUP BY t.hour
ORDER BY play_count DESC
LIMIT 1;
```

### User-Level Song Play Analysis

```sql
SELECT u.user_id, u.first_name, u.last_name, COUNT(*) as play_count
FROM songplays s
JOIN users u ON s.user_id = u.user_id
GROUP BY u.user_id, u.first_name, u.last_name
ORDER BY play_count DESC;
```

### Top Artists by Number of Plays
```sql
SELECT a.name, COUNT(*) as play_count
FROM songplays s
JOIN artists a ON s.artist_id = a.artist_id
GROUP BY a.name
ORDER BY play_count DESC;
```

### Number of Plays by Subscription Level
```sql
SELECT level, COUNT(*) as play_count
FROM songplays
GROUP BY level
ORDER BY play_count DESC;
```

