
from multiprocessing import Pool
import multiprocessing
import subprocess
import re
from lib.rdd_creator import RDDCreator
from lib.file_finder import FileFinder
from lib.schema import song_schema
from pyspark.sql import SparkSession
import os

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

def main():

  spark = SparkSession\
    .builder\
    .appName("dev_app")\
    .getOrCreate()\

  spark.sparkContext.setLogLevel("ERROR")

  artist_schema = ['artist_id', 'artist_name', 'artist_latitude', 'artist_longitude', 'artist_location']
  song_schema = ['song_id',  'title', 'artist_id', 'year', 'duration']
  
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

  spark.stop()






  







  

if __name__ == "__main__":
  main()