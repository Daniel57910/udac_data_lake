
from multiprocessing import Pool
import multiprocessing
import subprocess
from lib.file_finder import FileFinder
from lib.rdd_creator import RDDCreator
import os
from lib.schema import song_schema
from pyspark.sql import SparkSession


def extract_files_from_s3(directories):
    with Pool(processes=multiprocessing.cpu_count()) as pool:
      pool.map(fetch_files_from_s3, directories)

def fetch_files_from_s3(suffix):
  '''
  for the log and song directories that are in s3, downloads them to the tmp directory, 
  suffix is the log and song directory directory in s3
  '''
  local_path = os.getcwd() + '/tmp' + f'/{suffix}'
  subprocess.run(f'aws s3 sync s3://udacity-dend/{suffix} {local_path}', shell=True, check=True)


def return_file_names(directory):
  file_finder = FileFinder(os.getcwd() + f'/tmp/{directory}', '.json')
  print(list(file_finder.return_file_names()))
  return list(file_finder.return_file_names())

def create_schema_heirachy_from_data(name, schema):
  directory = {}
  directory[name] = list(schema.keys())
  return directory
  


def main():

  spark = SparkSession\
    .builder\
    .appName("dev_app")\
    .getOrCreate()\

  print('Building spark session at')

  spark.sparkContext.setLogLevel("DEBUG")
  print(spark)

  directories = ['song_data']
  dataframes = {}
  # extract_files_from_s3(directories)

  song_directory = return_file_names('song_data')
  print('\n'.join(song_directory))
  # frames = map(lambda dir: RDDCreator(dir, return_file_names(dir), spark), directories)

  # frames = map(
  #   lambda frame: frame.create_rdd_from_path(), frames
  # )

  # song_data = list(frames)[0]
  # print(song_data[0])

  # song_data = song_data.select(['artist_id'])
  # print(song_data.show())
  # song_data.write.csv(os.getcwd() + '/example.csv')
  # print('FILE WROTE TO CSV')

  spark.stop()


 




  







  

if __name__ == "__main__":
  main()