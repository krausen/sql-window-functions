# SQL Window function

Demonstrating the use of window functions in sql, initialize the db with `./init.sh` (will start a docker container with postgres) and connect with `psql -U postgres -h localhost`.


## Deduplicate and keeping the latest ingested value

```sql
SELECT COUNT(*) 
FROM 
    (SELECT *,
            ROW_NUMBER() OVER (PARTITION BY (event_id) ORDER BY ingestion_time DESC) AS rn 
    FROM streams) dedup 
WHERE rn=1;
```

## Running count for number of streams

```sql
SELECT
    event_id,
    song_name,
    stream_started.
    COUNT(*) OVER (PARTITION BY song_id ORDER BY stream_started ROWS BETWEEN UNBOUNDED
    PRECEDING and CURRENT ROW) AS running_total_streams
FROM streams
ORDER BY stream_started;
```

## Show previous and next stream time for song 0

```sql
SELECT
    event_id,
    song_name,
    stream_started,
    LAG(stream_started, 1) OVER (ORDER BY stream_started) as previous_stream,
    LEAD(stream_started, 1) OVER (ORDER BY stream_started) as previous_stream
FROM streams
WHERE song_id = 0;
```

## Show total number of streams for the song and the total number of streams in the channel

```sql
SELECT
    event_id,
    song_name,
    stream_started,
    COUNT(song_id) OVER (PARTITION BY song_id) as total_streams_of_this_song, 
    COUNT(song_id) OVER (PARTITION BY channel) as total_streams_in_channel
FROM streams;
```