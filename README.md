# mqtt2sql

This [python](#deprecated) program creates copies of MQTT broker/server payloads into a SQL database (currently supports MySQL5.x-8.x/MariaDB 10.x and SQLite 3).

[![master](https://img.shields.io/badge/master-v3.0.2-blue.svg)](https://github.com/curzon01/mqtt2sql/tree/master)
[![License](https://img.shields.io/github/license/curzon01/mqtt2sql.svg)](LICENSE)

If you like **mqtt2sql** give it a star or fork it:

[![GitHub stars](https://img.shields.io/github/stars/curzon01/mqtt2sql.svg?style=social&label=Star)](https://github.com/curzon01/mqtt2sql/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/curzon01/mqtt2sql.svg?style=social&label=Fork)](https://github.com/curzon01/mqtt2sql/network)

The MQTT data are provided in the following tables/view:

* Table `mqtt`  
contains the last MQTT copied payload for the subcribed topic
* Table `mqtt_history`  
contains the payloads history from `mqtt`. History data can be disabled by topic or in general (see [History control](#history-control)).
* View `mqtt_history_view`  
contains data from `mqtt_history` with readable topics and timestamps (see [History view](#history-view))

<details>
  <summary>Contents</summary>

* [Installation](#installation)
  * [Python prerequisites](#python-prerequisites)
  * [MySQL prerequisites](#mysql-prerequisites)
  * [Copy the program](#copy-the-program)
  * [Create database objects](#create-database-objects)
* [Usage](#usage)
  * [Start from command line](#start-from-command-line)
  * [Start as systemd manager daemon](#start-as-systemd-manager-daemon)
* [History data](#history-data)
  * [History control](#history-control)
  * [History view](#history-view)
  * [Localized timestamps](#localized-timestamps)
* [Deprecated](#deprecated)
  * [Deprecated program arguments](#deprecated-program-arguments)
</details>

## Installation

During the installation we

* create a usable [Python 3.x](https://www.python.org/downloads/) environment
* create the necessary databases and objects
* test the program
* and if desired, create a system daemon

### Python prerequisites

If not already done, install a working [Python 3.x](https://www.python.org/downloads/) environment described there.

**Note**: Due to the [Python 2.7 EOL](https://github.com/python/devguide/pull/344) in Jan 2020 Python 2.x is no longer supported.

Install Pip, Paho MQTT and MySQLdb lib to your python environment use

```bash
sudo apt install python3 python3-pip libmysqlclient-dev
python -m pip install -r requirements.txt
```

**Check** that Python 3.x is installed e.g.

```bash
$ python --version
Python 3.10.6
```

**Check** that pip installed **pao-mqtt greater or equal version 1.2.3**, e.g.

```bash
$ python -m pip show paho-mqtt
...
Name: paho-mqtt
Version: 1.6.1
...
```

### MySQL prerequisites

In order for MySQL to handle time zones correctly, make sure that the [MySQL Server Time Zone Support](https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html) is set up correctly (see also [Populating the Time Zone Tables](https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-installation)).

### Copy the program

Copy repository using git and make the program executable:

```bash
git clone https://github.com/curzon01/mqtt2sql
cd mqtt2sql
chmod +x mqtt2sql.py
```

### Create database objects

> the sql scripts we use here for MySQL and SQLite are creating all neccessary databases and objects. The default database is `mqtt` and the tables are `mqtt_history` with history data enabled. If you want to use different namings or existing databases, edit the top of the related *sql script before using it.

#### Using MySQL

```bash
mysql --host localhost < mysql.sql
```

If a username and password is set on your server, use

```bash
mysql --host localhost -u <username> -p < mysql.sql
```

#### Using SQLite3

```bash
sqlite3 mqtt.db <sqlite.sql
```

## Usage

### Start from command line

For first help, start the script with parameter -h

```bash
./mqtt2sql.py -h
```

If you got a help page, you can start try to run it using one of the existing database objects above

#### Run program using MySQL

> Change parameter to your needs

```bash
./mqtt2sql.py --mqtt mqtt://mqttuser:mqttpasswd@localhost/mytopic/# \
--sql-type mysql --sql-host localhost --sql-username sqluser --sql-password 'sqlpasswd' --sql-db mqtt -v
```

#### Run program using SQLite3

> Change parameter to your needs

```bash
./mqtt2sql.py --mqtt mqtt://mqttuser:mqttpasswd@localhost/mytopic/# \
--sql-type sqlite --sql-db mqtt.db -v
```

### Start as systemd manager daemon

The program allows the entire program parameters to be transferred in a configuration file instead of as individual program parameters.  
For the following service file we use a copy of the configuration file [mqtt2sql.conf](https://github.com/curzon01/mqtt2sql/blob/master/mqtt2sql.conf) for parameterization and chnage it to our needs. This means that we do not have to edit the service file in the case of changes.

#### Make a copy of the program and configuration file and edit the parameter

```bash
sudo mkdir -p /usr/local/bin/
sudo cp mqtt2sql.py /usr/local/bin/
sudo mkdir -p /usr/local/etc/
sudo cp mqtt2sql.conf /usr/local/etc/
sudo nano /usr/local/etc/mqtt2sql.conf
```

edit the configuration parameter for your needs and save it with `Ctrl+o` `Ctrl+x`.

#### Create mqtt2sql.service

```bash
sudo nano /etc/systemd/system/mqtt2sql.service
```

Insert the following lines

```conf
Description=MQTT2SQL
After=local-fs.target network.target mysql.service

[Service]
Type=simple
Restart=always
RestartSec=10
ExecStart=/usr/local/bin/mqtt2sql.py --configfile /usr/local/etc/mqtt2sql.conf

[Install]
WantedBy=multi-user.target
```

#### Reload systemd manager, restart daemon and check success

```bash
sudo systemctl daemon-reload
sudo systemctl restart mqtt2sql
sudo systemctl status mqtt2sql
```

#### Finally enable the service

```bash
sudo systemctl enable mqtt2sql
```

## History data

Table `mqtt_history` contains data history from table `mqtt` changes received by the MQTT subscription. The default setup is storing only changed values within `mqtt_history`.

Database objects created by this scripts enables history data as default.

### History control

History data creation depends on two columns for topics in `mqtt` table:

* column `history_enable` indicates whether topic payload is saved in `mqtt_history` (1) or not (0).
* column `history_diffonly` indicates whether topic payload is saved in `mqtt_history` only if payload is different to previously (1) or always (0). Note: this setting is ignored if `history_enable` is 0.

#### Change history control for exiting records

For existing `mqtt` table records use the UPDATE command, e.g. `UPDATE mqtt SET history_enable=0` to disable history saving for all existing topic records (accordingly same sing column `history_diffonly`)

#### Change history control for newly created records

For newly created `mqtt` table records change the default of the related column using the ALTER command, e.g.

```sql
ALTER TABLE `mqtt`
    CHANGE COLUMN `history_enable`
    `history_enable` TINYINT(4) NOT NULL DEFAULT 0;`
```

set the same as above (disable history saving for topic records) for newly created topics.

### History view

The view `mqtt_history_view` can be used to get the history data with human readable topics instead of foreign keys from original table `mqtt_history`. The view has also two timestamp columns:

* `ts` is the timestamp from lastest insert into the `mqtt_history` table
* `ts_last` is the timestamp from lastest change

If `history_diffonly` is enabled (1), `ts` shows the timestamp of the last payload change where the `ts_last` shows the latest recevied timestamp (independent if the last recevied payload has change or not).

### Localized timestamps

Since *mqtt2sql* v3.0.0, timestamps are saved using [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time) by default. This is the best way to avoid time duplicates during the change from e. g. daylight saving time (DST) to standard time.

#### SQLite

To process timestamps under `SQLite` using your local time, convert them when reading the database (e.g. `SELECT DATETIME(ts, 'localtime'`).

#### MySQL

When using `MySQL` you do not need to perform an explicit conversion when reading table. `MySQL` continues to use the `TIMESTAMP` data type for timestamps, which always stores timestamps in UTC and automatically converts them to the local time zone of the client connection when they are returned. For this purpose, *mqtt2sql* uses the same time zone setting for the connection to the MySQL server as the value that is saved when insert the data.

Leave the setting for `--sql-timezone` at the default value `UTC`, because Python handles the time conversion from local time to UTC without errors, while MySQL works incorrectly.

If you want to use local timestamps for any reason, use the optional parameter `--sql-timezone <timezone>` (possible time zones can be displayed using `--sql-timezone help`). But be aware that this will result in ambiguous records regarding the time stamps during the changeover from summer to standard time. In this case, you can recognize the duplicate timestamps by the table `mqtt_history.id`, which are in ascending order.

## Deprecated

Due to the [Python 2.7 EOL](https://github.com/python/devguide/pull/344) in Jan 2020 Python 2.x is no longer supported.

### Deprecated program arguments

The following program arguments are deprecated but still valid and working for backwards compatibility.
However, these should no longer be used, as they could be omitted in the future.

#### Deprecated MQTT arguments

These deprecated parameters override MQTT URL components:

`--mqtt-host`, `--mqtthost`  
`--mqtt-port`, `--mqttport`  
`--mqtt-username`, `--mqttusername`  
`--mqtt-password` , `--mqttpassword`  
