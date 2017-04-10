# mqtt2mysql
This python script subscribes to MQTT broker topic and inserts the topic into a mysql database table

## Installation
### Prerequisite
* Paho MQTT and MySQLdb python lib are neccessary. To install it to you python environment use
```
sudo apt-get install python-pip python-mysqldb
sudo pip install paho-mqtt argparse
```
* Create database table using SQL command
```
CREATE TABLE `mqtt` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`topic` TEXT NOT NULL,
	`value` LONGTEXT NOT NULL,
	PRIMARY KEY (`topic`(1024)),
	INDEX `id` (`id`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB;
```
### Copy the program
* Copy repository using git:
```
git clone https://github.com/curzon01/mqtt2mysql
```
* Make it executable:
```
chmod +x mqtt2mysql/mqtt2mysql.py
```

## Usage
For first help, start the script with parameter -h
```
mqtt2mysql/mqtt2mysql.py -h
```
### Systemd manager daemon example
Create mqtt2mysql.service
```
sudo nano /etc/systemd/system/mqtt2mysql.service
```
Insert the following - replace example usernames and passwords with yours
```
Description=MQTT2MySQL
After=local-fs.target network.target mysql.service
 
[Service]
Type=simple
Restart=always
RestartSec=10
ExecStart=/usr/local/bin/mqtt2mysql.py --host phmqtt.project-home.local --username mqttuser --password mqttpasswd --topic myhome/# --sql_username mysqluser --sql_password 'mysqlpasswd' --sql_db mydb --logfile /var/log/mqtt.log

[Install]
WantedBy=multi-user.target
```
Finally reload systemd manager
```
sudo systemctl daemon-reload
```
