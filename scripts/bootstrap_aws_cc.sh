# !/bin/bash

sudo apt-get install python3 python3-pip libffi-dev python3-dev;
sudo pip3 install -r requirements.txt;
sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8980;
sudo iptables-save;
cd redis; make -j4; cd ..;
sudo sh -c "echo ' * hard nofile 64000 ' >> /etc/security/limits.conf";
sudo sh -c "echo ' * soft nofile 64000 ' >> /etc/security/limits.conf";
sudo shutdown -r 0;