/*
    sqlite.sql - mqtt2sql SQlite3 database objects

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
DROP TRIGGER IF EXISTS `mqtt_after_insert`;
DROP TRIGGER IF EXISTS `mqtt_after_update`;
DROP VIEW IF EXISTS `mqtt_history_view`;
DROP TABLE IF EXISTS `mqtt_history`;
DROP TABLE IF EXISTS `mqtt`;
/* -------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS `mqtt` (
	`id` INTEGER PRIMARY KEY AUTOINCREMENT,
	`ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`topic` TEXT NOT NULL,
	`value` LONGTEXT NOT NULL,
	`qos` INTEGER NOT NULL,
	`retain` INTEGER NOT NULL,
	`history_enable` INTEGER NOT NULL DEFAULT 1,
	`history_diffonly` INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS `mqtt_id` ON `mqtt` ( `id` );
CREATE UNIQUE INDEX IF NOT EXISTS `mqtt_topic` ON `mqtt` ( `topic` );

CREATE TABLE IF NOT EXISTS `mqtt_history` (
	`id` INTEGER PRIMARY KEY AUTOINCREMENT,
	`ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`topicid` INTEGER UNSIGNED NOT NULL,
	`value` LONGTEXT NULL DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS `mqtt_history_topicid` ON `mqtt_history` ( `topicid` );
CREATE INDEX IF NOT EXISTS `mqtt_history_ts` ON `mqtt` ( `ts` );


CREATE TRIGGER IF NOT EXISTS `mqtt_after_insert` AFTER INSERT ON `mqtt`
FOR EACH ROW
WHEN NEW.history_enable
BEGIN
    INSERT INTO mqtt_history (ts, topicid, value) VALUES(NEW.ts, NEW.id, NEW.value);
END;

CREATE TRIGGER IF NOT EXISTS `mqtt_after_update` AFTER UPDATE ON `mqtt`
FOR EACH ROW
WHEN NEW.history_enable AND (NEW.history_diffonly=0 OR (NEW.history_diffonly=1 AND OLD.value!=NEW.value))
BEGIN
	INSERT INTO mqtt_history (ts, topicid, value) VALUES(NEW.ts, NEW.id, NEW.value);
END;


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
