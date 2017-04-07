# mqtt2mysql
This python script subscribes to MQTT broker topic and inserts the topic into a mysql database table

## Installation
* Copy repository using git:
```
git clone https://github.com/curzon01/mqtt2mysql
```
* Make it executable:
```
chmod +x mqtt2mysql/mqtt2mysql.py
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

## Usage
For help start the script with parameter -h
```
mqtt2mysql/mqtt2mysql.py -h
```
