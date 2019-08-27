'''
File containing the redshift schema
List of tables loaded into etl.py
All tables dropped then created
Review diagrams in README for relational model
'''

song_staging_table = '''CREATE TABLE IF NOT EXISTS song_staging
(artist_id TEXT not null,
artist_latitude NUMERIC,
artist_location TEXT,
artist_longitude NUMERIC,
artist_name TEXT not null,
duration NUMERIC,
num_songs INT,
song_id TEXT not null,
title TEXT not null,
year INT);'''

log_staging_table = '''CREATE TABLE IF NOT EXISTS log_staging
(artist TEXT,
auth TEXT,
first_name TEXT,
gender TEXT,
item_in_session INT, 
last_name TEXT,
length NUMERIC,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration NUMERIC,
session_id INT,
song_name TEXT,
status INT,
timestamp BIGINT,
user_agent TEXT,
user_id TEXT);'''

d_artist_table = '''CREATE TABLE IF NOT EXISTS d_artist
(artist_key BIGINT identity(0, 1),
artist_id TEXT not null,
artist_latitude NUMERIC,
artist_location TEXT,
artist_longitude TEXT,
artist_name TEXT not null,
PRIMARY KEY(artist_key))
COMPOUND SORTKEY(artist_key, artist_name);'''

d_song_table = '''CREATE TABLE IF NOT EXISTS d_song
(song_key BIGINT identity(0, 1),
song_id TEXT,
title TEXT,
duration NUMERIC,
year INT,
artist_id TEXT REFERENCES d_artist(artist_key),
PRIMARY KEY(song_key))
COMPOUND SORTKEY(song_key, title);'''

d_app_user_table_staging = '''CREATE TABLE IF NOT EXISTS d_app_user_staging
(app_user_key BIGINT identity(0, 1),
app_user_id INT,
first_name TEXT,
last_name TEXT,
gender TEXT,
level TEXT,
timestamp BIGINT,
PRIMARY KEY(app_user_id))
COMPOUND SORTKEY(app_user_id, timestamp);'''

d_app_user_table = '''CREATE TABLE IF NOT EXISTS
d_app_user(
app_user_key BIGINT identity(0, 1),
app_user_id INT,
first_name TEXT,
last_name TEXT,
gender TEXT,
level TEXT,
PRIMARY KEY(app_user_id))
COMPOUND SORTKEY(app_user_key, first_name, last_name);'''

d_timestamp_table = '''CREATE TABLE IF NOT EXISTS
d_timestamp(
timestamp_key BIGINT identity(0, 1),
year INT, 
month INT,
day INT,
minute INT,
second INT,
hour INT,
weekday BOOL,
timestamp BIGINT,
PRIMARY KEY(timestamp_key))
SORTKEY(timestamp);'''

f_songplay_table = '''CREATE TABLE IF NOT EXISTS
f_songplay(
songplay_key BIGINT identity(0, 1),
start_time BIGINT,
user_id INT REFERENCES d_app_user(app_user_id),
level TEXT,
song_key INT REFERENCES d_song(song_key),
artist_key INT REFERENCES d_artist(artist_key),
session_id INT, 
location TEXT,
user_agent TEXT,
PRIMARY KEY(songplay_key));'''


table_commands = [
  'DROP TABLE IF EXISTS song_staging;', 
  'DROP TABLE IF EXISTS log_staging;', 
  'DROP TABLE IF EXISTS d_artist;', 
  'DROP TABLE IF EXISTS d_song;',
  'DROP TABLE IF EXISTS d_app_user_staging;',
  'DROP TABLE IF EXISTS d_app_user;',
  'DROP TABLE IF EXISTS d_app_user_id;',
  'DROP TABLE IF EXISTS d_timestamp;',
  'DROP TABLE IF EXISTS f_songplay'
]

table_commands.reverse()

create_all_tables = [
  song_staging_table, 
  log_staging_table, 
  d_artist_table, 
  d_song_table, 
  d_app_user_table_staging, 
  d_app_user_table, 
  d_timestamp_table, 
  f_songplay_table
]

table_commands.extend(create_all_tables)

