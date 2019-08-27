zip -r project.zip *.py lib/*.py requirements.txt make_venv.sh unzip.sh
scp -i sparkify-lake-2.pem project.zip hadoop@ec2-54-234-162-226.compute-1.amazonaws.com:~
rm project.zip
