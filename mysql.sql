/*
    mysql.sql - mqtt2sql MySQL database objects

    Copyright (C) 2020 Norbert Richter <nr@prsolution.eu>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

Usage:
    See README.md for more details how to install and use
    https://github.com/curzon01/mqtt2sql#mqtt2sql
*/


/* Comment out or adapt the following lines if you want to use another
 * existing database and objects
 */
CREATE schema IF NOT EXISTS mqtt;
USE mqtt;

DROP TRIGGER IF EXISTS `mqtt_before_insert`;
DROP TRIGGER IF EXISTS `mqtt_after_insert`;
DROP TRIGGER IF EXISTS `mqtt_after_update`;
DROP VIEW IF EXISTS `mqtt_history_view`;
DROP TABLE IF EXISTS `mqtt_history`;
DROP TABLE IF EXISTS `mqtt`;
/* -------------------------------------------------------------------- */


/* Create database objects */

CREATE TABLE IF NOT EXISTS `mqtt` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`topic` TEXT NOT NULL,
	`value` LONGTEXT NOT NULL,
	`qos` TINYINT(3) UNSIGNED NOT NULL,
	`retain` TINYINT(3) UNSIGNED NOT NULL,
	`history_enable` TINYINT(4) NOT NULL DEFAULT 1,
	`history_diffonly` TINYINT(4) NOT NULL DEFAULT 1,
	PRIMARY KEY (`topic`(255)),
	UNIQUE INDEX `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `mqtt_history` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`topicid` INT(10) UNSIGNED NOT NULL,
	`value` LONGTEXT NULL DEFAULT NULL,
	PRIMARY KEY (`id`),
	INDEX `topicid` (`topicid`),
	INDEX `ts` (`ts`),
	CONSTRAINT `FK_mqtt_history_mqtt` FOREIGN KEY (`topicid`) REFERENCES `mqtt` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DELIMITER //
CREATE TRIGGER IF NOT EXISTS `mqtt_before_insert` BEFORE INSERT ON `mqtt` FOR EACH ROW BEGIN
    DECLARE new_id INTEGER;

    SET new_id=(select max(id)+1 from mqtt);
    IF NEW.id != new_id THEN
        SET NEW.id = new_id;
    END IF;
END//
DELIMITER ;

DELIMITER //
CREATE TRIGGER IF NOT EXISTS `mqtt_after_insert` AFTER INSERT ON `mqtt` FOR EACH ROW BEGIN
    IF NEW.history_enable = 1 THEN
        INSERT INTO mqtt_history SET ts=NEW.ts, topicid=NEW.id, value=NEW.value;
    END IF;
END//
DELIMITER ;

DELIMITER //
CREATE TRIGGER IF NOT EXISTS `mqtt_after_update` AFTER UPDATE ON `mqtt` FOR EACH ROW BEGIN
    IF NEW.history_enable = 1 AND (NEW.history_diffonly = 0 OR (NEW.history_diffonly = 1 AND OLD.value != NEW.value)) THEN
        INSERT INTO mqtt_history SET ts=NEW.ts, topicid=NEW.id, value=NEW.value;
    END IF;
END//
DELIMITER ;


CREATE VIEW IF NOT EXISTS `mqtt_history_view` AS
SELECT
    h.id,
    h.ts AS 'ts',
    m.ts AS 'ts_last',
    m.topic,
    h.value
FROM mqtt_history h
INNER JOIN mqtt m
    ON m.id = h.topicid;
