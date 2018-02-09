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

cd mongo-raw-sink-pastebin
sh run-file.sh
cd ..

cd mongo-processed-sink-pastebin
sh run-file.sh
cd ..

cd pastebin-scraper-email-results
sh run-file.sh
cd ..

