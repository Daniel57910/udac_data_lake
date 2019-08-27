zip -r data.zip tmp/*
scp -i sparkify-lake-2.pem data.zip hadoop@ec2-54-234-162-226.compute-1.amazonaws.com:~

