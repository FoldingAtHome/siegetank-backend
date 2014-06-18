# !/bin/bash

sudo apt-get install git python3 python3-pip libffi-dev python3-dev;
sudo pip3 install -r requirements.txt;
sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8980;
cd redis; make -j4; cd ..;
sudo echo " * hard nofile 64000 " >> limits.conf;
sudo echo " * soft nofile 64000 " >> limits.conf;
sudo shutdown -r 0;