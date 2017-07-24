# mqtt2mysql
This python script subscribes to MQTT broker topic and inserts the topic into a SQL database table

## Installation
### Prerequisite
* Paho MQTT, MySQLdb and/or sqlite3 python lib are neccessary. To install it to you python environment use
```
sudo apt-get install python-pip python-mysqldb python-pysqlite2
sudo pip install paho-mqtt argparse
```
* Check that pip installed pao-mqtt greater or equal Version: 1.2.3, e.g.
```
$ sudo pip show paho-mqtt
Name: paho-mqtt
Version: 1.3.0
```
* Create database table using SQL command
#### MySQL
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
#### sqlite3
```
CREATE TABLE `mqtt` (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	`ts`	TEXT,
	`topic`	TEXT NOT NULL UNIQUE,
	`value`	TEXT NOT NULL
);
```


### Copy the program
* Copy repository using git:
```
git clone https://github.com/curzon01/mqtt2sql
```
* Make it executable:
```
chmod +x mqtt2sql/mqtt2sql.py
```

## Usage
For first help, start the script with parameter -h
```
mqtt2sql/mqtt2sql.py -h
```
### Systemd manager daemon example
Create mqtt2sql.service
```
sudo nano /etc/systemd/system/mqtt2sql.service
```
Insert the following lines - note: replace example usernames and passwords with yours
```
Description=MQTT2SQL
After=local-fs.target network.target mysql.service
 
[Service]
Type=simple
Restart=always
RestartSec=10
ExecStart=/usr/local/bin/mqtt2sql.py --mqtthost phmqtt.myhome.local --mqttusername mqttuser --mqttpassword mqttpasswd --topic myhome/# --sqlhost localhost --sqlusername sqluser --sqlpassword 'sqlpasswd' --sqldb mydb --logfile /var/log/mqtt.log

[Install]
WantedBy=multi-user.target
```
Reload systemd manager, restart daemon and check succes
```
sudo systemctl daemon-reload
sudo systemctl restart mqtt2sql
sudo systemctl status mqtt2sql
```
Finally be sure the service is enabled:
```
sudo systemctl enable mqtt2sql
```
