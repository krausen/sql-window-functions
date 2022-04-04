from datetime import date
from faker import Faker
from faker.providers import date_time
import csv
import hashlib
import psycopg2
from random import randrange


def _hash(s):
    h = hashlib.new("sha256")
    h.update(bytes(s, "utf-8"))
    return h.hexdigest()

def _get_random_song(songs):
    song_id = randrange(len(songs))
    artist, song_name, channel = songs[song_id]
    return song_id, artist, song_name, channel

def _get_random_user():
    user_names = ["Raymond Tucker","Heather Kelly","Randy Alexander","Allen Wilson","Carrie Braun","Jamie Williams","Joseph Garrison","Kathy Moon","Gregory Moran","Cheyenne Madden"]
    user = user_names[randrange(len(user_names))]
    return user

def _get_random_date():
    fake = Faker()
    fake.add_provider(date_time)
    return fake.date_time_between_dates(date(2022,1,1), date(2022,12,31))

def _populate_database(n_rows, songs, duplicates=False):
    with psycopg2.connect("dbname=postgres user=postgres host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS streams (event_id varchar, user_id varchar, song_id int, artist_name varchar, song_name varchar, channel varchar, stream_started timestamp, ingestion_time timestamp);")
            for i in range(n_rows):
                user_id = _hash(_get_random_user())
                song_id, artist_name, song_name, channel = _get_random_song(songs)
                stream_started = _get_random_date()
                event_id = _hash(f"{user_id}:{song_id}:{stream_started}")
                cur.execute(f"INSERT INTO streams (event_id, user_id, song_id, artist_name, song_name, channel, stream_started, ingestion_time) VALUES (%s, %s, %s, %s, %s, %s, %s, current_timestamp);", (event_id, user_id, song_id, artist_name, song_name, channel, stream_started))
                if duplicates and i % 10 == 0:
                    # Intentionally insert duplicates every 10 rows
                    cur.execute(f"INSERT INTO streams (event_id, user_id, song_id, artist_name, song_name, channel, stream_started, ingestion_time) VALUES (%s, %s, %s, %s, %s, %s, current_timestamp);", (event_id, user_id, song_id, artist_name, song_name, channel, stream_started))

if __name__ == "__main__":
    with open("songs.csv") as csv_file:
        songs = list(csv.reader(csv_file, delimiter=","))
    _populate_database(10**3, songs)