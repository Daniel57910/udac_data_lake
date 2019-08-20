
from multiprocessing import Pool
import multiprocessing
import subprocess
import re
from joblib import Parallel, delayed
# from lib.file_finder import FileFinder
# from lib.data_loader import DataLoader
import os
import pdb
import csv
from datetime import datetime
import pandas as pd
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

def main():

  directories = ['log_data', 'song_data']
  # extract_files_from_s3(directories)

  spark = SparkSession.builder.getOrCreate()
  log_data = spark.read.json(os.getcwd() + '/tmp/log_data/*', multiLine=True)




  

if __name__ == "__main__":
  main()