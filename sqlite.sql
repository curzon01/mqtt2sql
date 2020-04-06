DROP TRIGGER IF EXISTS `mqtt_after_insert`;
DROP TRIGGER IF EXISTS `mqtt_after_update`;
DROP VIEW IF EXISTS `mqtt_history_view`;
DROP TABLE IF EXISTS `mqtt_history`;
DROP TABLE IF EXISTS `mqtt`;

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
