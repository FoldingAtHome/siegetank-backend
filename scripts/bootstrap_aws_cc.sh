# !/bin/bash

cd redis; make -j4; cd ..;
sudo apt-get install git python3 python3-pip libffi-dev python3-dev;
sudo pip3 install -r requirements.txt;
sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8980;