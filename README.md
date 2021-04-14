# EdgeNetworkMonitor
# Required
* Gathering Device - Device that will be used to sniff the network and gather data. For example, I used a Raspberry Pi 3B.
  * Micro SD Card if you're using the Raspberry Pi 3B.
  * Kali Linux Image - https://www.offensive-security.com/kali-linux-arm-images/ (Raspberry Pi 3B)
* Monitoring Device - Device that will be used to collect the gathered data and monitor it using queries. For example, I used my personal computer.

# Setup
For the setup, I will explain the setup I use for my Raspberry Pi 3B and personal computer (Windows 10 Home, 64-bit operating system).

Raspberry Pi 3B setup:

Format the micro sd card to exFat.

Flash Kali Linux Img onto your micro sd card and insert it into the Raspberry Pi 3B.
  - I used balenaEtcher to flash img onto micro sd card ~ https://www.balena.io/etcher/
  - I used the image file kali-linux-2020.3a-rpi3-nexmon.img, but using a new version may work.

Connect to the Raspberry Pi 3B and run the following commands in the terminal:
  - sudo apt-get install postgresql-11
  - sudo apt-get install tshark
  - sudo apt-get install python3
  - sudo dpkg-reconfigure openssh-server
  - airmon-ng start wlan0
  - python3 -m pip install asciimatics==1.11.0
  - python3 -m pip install pyshark==0.4.2.11
  - sudo python3 -m pip install asciimatics==1.11.0
  - sudo python3 -m pip install pyshark==0.4.2.11

Setup PSQL with database and tables:
  - cd /etc/ssl/certs/
  - sudo make-ssl-cert generate-default-snakeoil --force-overwrite
  - sudo service postgresql start
  - sudo -u postgres -i
  - psql
  - create user kali with password 'password' createdb superuser;
  - create database network_stream owner kali;
  - \q
  - exit
  - psql network_stream
  - create table network_log_batch(
      timestamp double precision unique not null,      
      src text not null,      
      srcport integer,      
      dst text not null,      
      dstport integer,      
      protocol text,      
      length integer      
    );
  - create table network_log_summary(
      id serial primary key,
      min_timestamp double precision not null,
      max_timestamp double precision not null,
      src text not null,
      srcport integer,
      dst text not null,
      dstport integer,
      protocol text,
      min_length integer,
      max_length integer,
      avg_length numeric,
      summ_size integer
    );
