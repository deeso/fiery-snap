# cleanup local directory
sudo rm -r ./dist/ ./build/
sudo rm -r /usr/local/lib/python2.7/dist-packages/fiery_snap-1.0-py2.7.egg
sudo rm -r ./src/*.egg-info/
find . -name \*.pyc -delete
find . -name \*.swp -delete
find . -name \*.swo -delete
