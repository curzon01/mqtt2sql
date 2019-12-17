# mqtt2sql
This python 3 script subscribes to MQTT broker topic and inserts the topic into a SQL database table

# Content
* [Installation](#installation)
* [Usage](#usage)
* [Store history data](#history-data)

## Installation
### Prerequisite
This script runs under [Python 3.x](https://www.python.org/downloads/).

**Note**: Due to the [Python 2.7 EOL](https://github.com/python/devguide/pull/344) in Jan 2020 Python 2.x is no longer supported.

* To install Pip, Paho MQTT and MySQLdb lib to your python environment use
```
sudo apt-get install python-pip3 python3-mysqldb
pip3 install paho-mqtt configargparse
```
* __Check__ that Python 3.x is installed e.g.
```
$ python3 --version
Python 3.8.0
```

* __Check__ that pip installed __pao-mqtt greater or equal version 1.2.3__, e.g.
```
$ pip3 show paho-mqtt
...
Name: paho-mqtt
Version: 1.5.0
...
```
* Create database table using SQL command
#### MySQL
```
CREATE TABLE `mqtt` (
	`id`     SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
	`ts`     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`active` TINYINT(4) NOT NULL DEFAULT '1',
	`topic`  TEXT NOT NULL,
	`value`  LONGTEXT NOT NULL,
	`qos`    TINYINT(3) UNSIGNED NOT NULL DEFAULT '0',
	`retain` TINYINT(3) UNSIGNED NOT NULL DEFAULT '0',
	PRIMARY KEY (`topic`(1024)),
	INDEX `id` (`id`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB;
```
#### sqlite3
```
CREATE TABLE `mqtt` (
	`id`	 INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	`ts`	 TEXT,
	`active` INTEGER,
	`topic`	 TEXT NOT NULL UNIQUE,
	`value`	 TEXT NOT NULL,
	`qos`    INTEGER,
	`retain` INTEGER
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

## Store history data
If you need history data you can use SQL trigger on the basic table `mqtt` 
to record each inserted/updated record into a extra history table `mqtt_history`.

The two triggers `mqtt_after_insert` and `mqtt_after_update` will store each
insert or update on table `mqtt` into history table `mqtt_history`

### Create MySQL history table and trigger

```
CREATE TABLE `mqtt_history` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`topic_id` SMALLINT(5) UNSIGNED NOT NULL,
	`value` LONGTEXT NOT NULL,
	PRIMARY KEY (`id`),
	INDEX `topicid` (`topicid`),
	INDEX `ts` (`ts`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB;

DELIMITER //
CREATE TRIGGER `mqtt_after_insert` AFTER INSERT ON `mqtt` FOR EACH ROW BEGIN
	INSERT INTO mqtt_history SET ts=NEW.ts, topic_id=NEW.id, value=NEW.value;
END//
CREATE TRIGGER `mqtt_after_update` AFTER UPDATE ON `mqtt` FOR EACH ROW BEGIN
	INSERT INTO mqtt_history SET ts=NEW.ts, topic_id=NEW.id, value=NEW.value;
END//
DELIMITER ;
```
