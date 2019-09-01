
from multiprocessing import Pool
import multiprocessing
import subprocess
import re
from lib.rdd_creator import RDDCreator
from lib.file_finder import FileFinder
from lib.schema import schema
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

def make_parquet_file_directory(parquet_path):
  if not os.path.exists(parquet_path): os.makedirs(parquet_path)

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

def create_rdd_from_files(frames):
  return list(map(
    lambda frame: frame.create_rdd_from_path(), frames
  ))

def main():

  parquet_file_path = os.getcwd() + '/parquet_files'
  make_parquet_file_directory(parquet_file_path)

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  directories = ['log_data', 'song_data']
  # extract_files_from_s3(directories)

  frames = map(lambda dir: RDDCreator(dir, return_file_names(dir), spark), directories)

  log_frame, song_frame = create_rdd_from_files(frames)
  
  artist_subset = song_frame.select([col for col in schema['artist_schema']]).dropna().dropDuplicates()
  song_subset = song_frame.select([col for col in schema['song_schema']]).dropna().dropDuplicates()

  timestamp_subset = log_frame.select(['ts']).dropna()

  timestamp_data = [int(row.ts) for row in timestamp_subset.collect()]

  timestamp_subset = apply_transformation_to_dataframe(
    spark, timestamp_data, schema['timestamp_schema'], unpack_timestamp
  )

  app_user_subset = log_frame.select([col for col in schema['app_user_schema']]).dropna().orderBy('ts').dropDuplicates()

  songplay_subset = log_frame.select([col for col in schema['songplay_schema']])

  artist_and_song_subset = artist_subset.join(
    song_subset, artist_subset.artist_id == song_subset.artist_id).drop(song_subset.artist_id).select(
    [col for col in schema['artist_and_song_join']]
  )
  
  songplay_subset = songplay_subset.join(
    artist_and_song_subset, 
    [songplay_subset.artist == artist_and_song_subset.artist_name, songplay_subset.song == artist_and_song_subset.title], 
    how='left'
  ).select([col for col in schema['songplay_schema'] + ['artist_id', 'song_id']])

  frames_to_disk = [
    artist_subset, 
    song_subset,
    timestamp_subset,
    app_user_subset,
    songplay_subset
  ]

  parquet_file_names = [
    'd_artist',
    'd_song',
    'd_timestamp',
    'd_app_user',
    'f_songplay'
  ]

  parquet_files = dict(zip(parquet_file_names, frames_to_disk))

  for file in parquet_files:
    parquet_files[file].write.parquet(parquet_file_path + '/{}'.format(file))
    
  spark.stop()

if __name__ == "__main__":
  main()