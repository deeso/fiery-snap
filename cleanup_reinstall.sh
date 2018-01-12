# cleanup local directory
sudo rm -r ./dist/ ./build/
sudo rm -r /usr/local/lib/python2.7/dist-packages/fiery_snap-1.0-py2.7.egg
sudo rm -r ./src/fiery_snap.egg-info/
find . -name \*.pyc -delete

sudo python setup.py install
