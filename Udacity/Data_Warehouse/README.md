
# Sparkify ETL Pipeline and Database Design

## Table of Contents
1. [Reason and Justification for the Database Schema Design](#reason-and-justification-for-the-database-schema-design)
2. [Purpose of the Database in Context of Sparkify](#purpose-of-the-database-in-context-of-sparkify)
3. [Example Queries for Song Play Analysis](#example-queries-for-song-play-analysis)

---

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

