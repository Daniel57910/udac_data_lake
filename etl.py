
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
  '''
  Parralelize downloading the log and song directories from s3 to disk
  '''

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
  '''
  Returns a list of all log and song files, identified via json format
  Needed as cannot read directories straight into RDD as they are too nested
  '''

  file_finder = FileFinder(os.getcwd() + '/tmp/{}/'.format(directory), '*.json')
  return list(file_finder.return_file_names())

def make_parquet_file_directory(parquet_path):
  '''
  makes the directory for parquet_path on EMR master if it does not exist
  '''

  if not os.path.exists(parquet_path): 
    os.makedirs(parquet_path)

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
  '''
  creates and returns a dataframe from unpack_timestamp with schema defined in schema argument
  '''

  return spark.createDataFrame(
    list(map(
      func, data)
    ),
    schema = schema
  )

def create_rdd_from_files(frames):
  '''
  returns a list of rdd data structures from the frame objects list
  '''

  return list(map(
    lambda frame: frame.create_rdd_from_path(), frames
  ))

def main():

  parquet_file_path = os.getcwd() + '/tables'
  make_parquet_file_directory(parquet_file_path)

  spark = SparkSession\
    .builder\
    .appName("sparkify_etl")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  directories = ['log_data', 'song_data']
  
  extract_files_from_s3(directories)

  frames = map(lambda dir: RDDCreator(dir, return_file_names(dir), spark), directories)

  log_frame, song_frame = create_rdd_from_files(frames)
  
  artist_subset = song_frame.select(schema['artist_schema']).dropna().dropDuplicates(['artist_id'])
  song_subset = song_frame.select(schema['song_schema']).dropna().dropDuplicates(['song_id'])

  timestamp_subset = log_frame.select(['ts']).dropna().dropDuplicates(['ts'])

  '''required as only want the data and not columns to transform timestamp into required time data'''
  timestamp_data = [int(row.ts) for row in timestamp_subset.collect()]

  timestamp_subset = apply_transformation_to_dataframe(
    spark, timestamp_data, schema['timestamp_schema'], unpack_timestamp
  )

  '''order by ts and last occuring entry selected as will contain most accurate user data'''
  app_user_subset = log_frame.select(schema['app_user_schema']).dropna().orderBy('ts').dropDuplicates(['userId'])

  songplay_subset = log_frame.select(schema['songplay_schema'])
  
  songplay_subset = songplay_subset.filter(songplay_subset.page == 'NextSong')

  '''join artist and song data to identify artists and songs as a one to many relationship'''
  artist_and_song_subset = artist_subset.join(
    song_subset, artist_subset.artist_id == song_subset.artist_id).drop(song_subset.artist_id).select(
    schema['artist_and_song_join']
  )

  '''select all songplay data and left join artist and song dimensions where possible'''
  songplay_subset = songplay_subset.join(
    artist_and_song_subset, 
    [songplay_subset.artist == artist_and_song_subset.artist_name, songplay_subset.song == artist_and_song_subset.title], 
    how='left'
  ).select(schema['songplay_schema'] + ['artist_id', 'song_id'])

  songplay_subset = songplay_subset.join(
    timestamp_subset,
    songplay_subset.ts == timestamp_subset.ts
  ).drop(timestamp_subset.ts).select(
    schema['songplay_schema'] + ['artist_id', 'song_id', 'month', 'year']
  )

  artist_subset.write.mode('overwrite').parquet(parquet_file_path + '/d_artist')
  song_subset.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(parquet_file_path + '/d_song')
  timestamp_subset.write.mode('overwrite').partitionBy('year', 'month').parquet(parquet_file_path + '/d_timestamp')
  app_user_subset.write.mode('overwrite').parquet(parquet_file_path + '/d_app_user')
  songplay_subset.write.mode('overwrite').partitionBy('year', 'month').parquet(parquet_file_path + '/f_songplay')

  '''sync tables directory to s3'''
  # try:
  #   subprocess.run(
  #     'aws s3 sync {} s3://sparkify-load/'.format(parquet_file_path), shell=True, check=True
  #   )
  # except Exception as e:
  #   raise Exception('Unable to sync directories to s3: {}'.format(e))

  spark.stop()

if __name__ == "__main__":
  main()