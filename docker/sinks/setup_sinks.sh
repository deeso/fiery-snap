#cd flatfile-sink
#sh run-file.sh
#cd ..

cd mongo-raw-sink
sh run-file.sh
cd ..

cd mongo-processed-sink
sh run-file.sh
cd ..

cd twitter-scraper-email-results
sh run-file.sh
cd ..
