from pyspark.sql.types import *

song_schema = {}

song_schema['artist_rdd'] = StructType([
    StructField('artist_id',        StringType(), False),
    StructField('artist_name',      StringType(), False),
    StructField('artist_latitude',  DoubleType(), True),
    StructField('artist_longitude', DoubleType(), True),
    StructField('artist_location',  StringType(), True)
  ]
)

