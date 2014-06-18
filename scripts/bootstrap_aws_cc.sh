deploy.sh

cd redis; make -j4; cd ..;
sudo apt-get install git python3 python3-pip libffi-dev python3-dev;
sudo pip3 install -r requirements.txt;