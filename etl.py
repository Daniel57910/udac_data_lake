
from multiprocessing import Pool
import multiprocessing
import subprocess
import re
from lib.rdd_creator import RDDCreator
from lib.file_finder import FileFinder
from lib.schema import song_schema
from pyspark.sql import SparkSession
import os
from datetime import datetime

def extract_files_from_s3(directories):
    with Pool(processes=multiprocessing.cpu_count()) as pool:
      pool.map(fetch_files_from_s3, directories)

def fetch_files_from_s3(suffix):
  '''
  for the log and song directories that are in s3, downloads them to the tmp directory, 
  suffix is the log and song directory directory in s3
  '''
  local_path = os.getcwd() + '/tmp/{}'.format(suffix)
  subprocess.run('aws s3 sync s3://udacity-dend/{} {}'.format(suffix, local_path), shell=True, check=True)

def return_file_names(directory):
  file_finder = FileFinder(os.getcwd() + '/tmp/{}/'.format(directory), '*.json')
  return list(file_finder.return_file_names())

def unpack_timestamp(row):
  '''
  receives a timestamp and returns a list of time variables that match the d_timestamp table
  the timestamp is appended to the list as this is used to join d_timestamp on f_songplay
  example:
  row entered = 1541903636796
  returned = [2018, 11, 11, 33, 56, 2, True, 1541903636796]
  '''
 
  new_row = list(datetime.fromtimestamp(row // 1000).timetuple()[0: 7])
  new_row[-1] = new_row[-1] > 5
  new_row.append(row)
  return new_row

def apply_transformation_to_dataframe(spark, data, schema, func):

  return spark.createDataFrame(
    list(map(
      func, data)
    ),
    schema = schema
  )

def main():

  spark = SparkSession\
    .builder\
    .appName("dev_app")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  artist_schema = ['artist_id', 'artist_name', 'artist_latitude', 'artist_longitude', 'artist_location']
  song_schema = ['song_id',  'title', 'year', 'duration', 'artist_id']
  timestamp_schema = ['year', 'month', 'day', 'minute', 'second', 'hour', 'weekday', 'ts']
  app_user_schema = ['firstName', 'gender', 'lastName', 'level', 'location', 'userId', 'ts']
  songplay_schema = ['ts', 'userId', 'level', 'artist', 'song', 'sessionId', 'location', 'userAgent']

  directories = ['log_data', 'song_data']
  dataframes = {}
  # extract_files_from_s3(directories)

  frames = map(lambda dir: RDDCreator(dir, return_file_names(dir), spark), directories)

  frames = map(
    lambda frame: frame.create_rdd_from_path(), frames
  )

  log_frame, song_frame = list(frames)
  
  artist_subset = song_frame.select([col for col in artist_schema])
  song_subset = song_frame.select([col for col in song_schema])
  artist_subset = artist_subset.dropna().dropDuplicates()
  song_subset = song_subset.dropna().dropDuplicates()

  timestamp_subset = log_frame.select(['ts']).dropna()

  timestamp_data = [int(row.ts) for row in timestamp_subset.collect()]

  timestamp_subset = apply_transformation_to_dataframe(
    spark, timestamp_data, timestamp_schema, unpack_timestamp
  )

  app_user_subset = log_frame.select([col for col in app_user_schema])

  #may be better to sort files with file_finder to ensure loaded in correct order
  app_user_subset = app_user_subset.dropna().orderBy('ts').dropDuplicates()

  songplay_subset = log_frame.select([col for col in songplay_schema])
  artist_and_song_subset = artist_subset.join(
    song_subset, artist_subset.artist_id == song_subset.artist_id).drop(song_subset.artist_id).select(
    [col for col in ['artist_id', 'song_id', 'artist_name', 'title']]
  )
  
  songplay_subset = songplay_subset.join(
    artist_and_song_subset, 
    [songplay_subset.artist == artist_and_song_subset.artist_name, songplay_subset.song == artist_and_song_subset.title], 
    how='left'
  ).select([col for col in songplay_schema + ['artist_id', 'song_id']])

  print(songplay_subset.show())







  spark.stop()

if __name__ == "__main__":
  main()