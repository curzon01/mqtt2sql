#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
VER = '2.5.5'

"""
    mqtt2mysql.py - Copy MQTT topic payloads to MySQL/SQLite database

    Copyright (C) 2022 Norbert Richter <nr@prsolution.eu>

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


Requirements:
    Python, pip and modules:
    > apt install python3 python3-pip
    > python -m pip install -r requirements.txt

Usage:
    See README.md for more details how to install and use
    https://github.com/curzon01/mqtt2sql#mqtt2sql

"""

class ExitCode:
    """
    Program return codes
    """
    OK = 0
    MISSING_MODULE = 1
    MQTT_CONNECTION_ERROR = 2
    SQL_CONNECTION_ERROR = 3

def module_import_error(module):
    """
    Print suggestion for import failures

    @param module: import module name
    """
    err_str = str(module)
    print("{}. Try 'python -m pip install {}' to install".format(err_str, err_str.split(' ')[len(err_str.split(' '))-1]))
    sys.exit(9)

# pylint: disable=wrong-import-position
import os
import sys
try:
    import paho.mqtt.client as mqtt
    import time
    import datetime
    import signal
    import logging
    import configargparse
    import urllib
    import re
    from threading import get_ident, Thread, BoundedSemaphore
    from random import random
except ImportError as err:
    module_import_error(err)
try:
    import ssl
    MODULE_SSL_AVAIL = True
except ImportError:
    MODULE_SSL_AVAIL = False
try:
    import MySQLdb
    MODULE_MYSQLDB_AVAIL = True
except ImportError:
    MODULE_MYSQLDB_AVAIL = False
try:
    import sqlite3
    MODULE_SQLITE3_AVAIL = True
except ImportError:
    MODULE_SQLITE3_AVAIL = False
# pylint: enable=wrong-import-position

SCRIPTNAME = os.path.basename(sys.argv[0])
SCRIPTPID = os.getpid()
SQLTYPES = {'mysql':'MySQl', 'sqlite':'SQLite'}
ARGS = {}
DEFAULTS = {
    'configfile': None,
    'logfile': None,
    'debug': None,
    'verbose': None,

    'mqtt-url': 'mqtt://localhost/#',
    'mqtt-username': None,
    'mqtt-password': None,
    'mqtt-host': 'localhost',
    'mqtt-topic': None,
    'mqtt-exclude-topic': None,
    'mqtt-cafile': None,
    'mqtt-certfile': None,
    'mqtt-keyfile': None,
    'mqtt-insecure': False,
    'mqtt-keepalive': 60,
    'mqtt-connect-timeout':500,

    'sql-type': 'mysql' if MODULE_MYSQLDB_AVAIL else ('sqlite' if MODULE_SQLITE3_AVAIL else None),
    'sql-host': 'localhost',
    'sql-port': 3306,
    'sql-username': None,
    'sql-password': None,
    'sql-db': None,
    'sql-table': 'mqtt',
    'sql-max-connection': 50,
    'sql-connection-retry': 10,
    'sql-connection-retry-start-delay': 1,
    'sql-transaction-retry': 10
}

DEFAULT_PORT_MQTT = 1883
DEFAULT_PORT_MQTTS = 8883
DEFAULT_PORT_MYSQL = 3306

def parseargs():
    """
    Program argument parser

    @return:
        configargparse.parse_args() result
    """
    # pylint: disable=global-statement
    global VER
    global DEFAULTS
    global MODULE_SSL_AVAIL
    global MODULE_MYSQLDB_AVAIL
    global MODULE_SQLITE3_AVAIL
    # pylint: enable=global-statement

    class SmartFormatter(configargparse.HelpFormatter):
        def _split_lines(self, text, width):
            if text.startswith('R|'):
                return text[2:].splitlines()
            return configargparse.HelpFormatter._split_lines(self, text, width)

    parser = configargparse.ArgumentParser(\
            description='MQTT to MySQL bridge. Subscribes to MQTT broker topic(s) and write values into a database.',
            epilog="There are no required arguments, defaults are displayed using --help.",
            formatter_class=SmartFormatter
            )
    parser.add_argument(
        '-c', '--configfile',
        metavar="<filename>",
        dest='configfile',
        is_config_file=True,
        default=DEFAULTS['configfile'],
        help="Config file, can be used instead of command parameter (default {})".format(DEFAULTS['configfile'])
    )

    mqtt_group = parser.add_argument_group('MQTT Options')
    mqtt_group.add_argument(
        '--mqtt',
        metavar='<URL>',
        dest='mqtt_url',
        default=DEFAULTS['mqtt-url'],
        help="R|Specify specify user, password, hostname, port and topic at once as a URL (default '{}').\n"
        "The URL must be in the form:\n"
        "mqtt(s)://[username[:password]@]host[:port][/topic]".format(DEFAULTS['mqtt-url'])
    )
    mqtt_group.add_argument('--mqtt-host',dest='mqtt_host', help=configargparse.SUPPRESS)
    mqtt_group.add_argument('--mqtthost', '--host', dest='mqtt_host', help=configargparse.SUPPRESS)
    mqtt_group.add_argument('--mqtt-port', dest='mqtt_port', type=int, help=configargparse.SUPPRESS)
    mqtt_group.add_argument('--mqttport', '--port', dest='mqtt_port', type=int, help=configargparse.SUPPRESS)
    mqtt_group.add_argument('--mqtt-username', dest='mqtt_username', help=configargparse.SUPPRESS)
    mqtt_group.add_argument('--mqttusername', '--username', dest='mqtt_username', help=configargparse.SUPPRESS)
    mqtt_group.add_argument('--mqtt-password', dest='mqtt_password',help=configargparse.SUPPRESS)
    mqtt_group.add_argument('--mqttpassword', '--password', dest='mqtt_password', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-topic',
        metavar='<topic>',
        dest='mqtt_topic',
        nargs='*',
        default=DEFAULTS['mqtt-topic'],
        help="optional: topic(s) to use (default {}).".format(DEFAULTS['mqtt-topic']))
    mqtt_group.add_argument('--topic', dest='mqtt_topic', nargs='*', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-exclude-topic',
        metavar='<topic>',
        dest='mqtt_exclude_topic',
        nargs='*',
        default=DEFAULTS['mqtt-exclude-topic'],
        help="optional: topic(s) to exclude (default {}).".format(DEFAULTS['mqtt-exclude-topic']))
    mqtt_group.add_argument(
        '--mqtt-cafile',
        metavar='<cafile>',
        dest='mqtt_cafile',
        default=DEFAULTS['mqtt-cafile'],
        help="cafile (default {})".format(DEFAULTS['mqtt-cafile']) if MODULE_SSL_AVAIL else configargparse.SUPPRESS)
    mqtt_group.add_argument('--cafile', dest='mqtt_cafile', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-certfile',
        metavar='<certfile>',
        dest='mqtt_certfile',
        default=DEFAULTS['mqtt-certfile'],
        help="certfile (default {})".format(DEFAULTS['mqtt-certfile']) if MODULE_SSL_AVAIL else configargparse.SUPPRESS)
    mqtt_group.add_argument('--certfile', dest='mqtt_certfile', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-keyfile',
        metavar='<keyfile>',
        dest='mqtt_keyfile',
        default=DEFAULTS['mqtt-keyfile'],
        help="keyfile (default {})".format(DEFAULTS['mqtt-keyfile']) if MODULE_SSL_AVAIL else configargparse.SUPPRESS)
    mqtt_group.add_argument('--keyfile', dest='mqtt_keyfile', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-insecure',
        dest='mqtt_insecure',
        action='store_true',
        default=DEFAULTS['mqtt-insecure'],
        help="suppress TLS verification (default {})".format(DEFAULTS['mqtt-insecure']) if MODULE_SSL_AVAIL else configargparse.SUPPRESS)
    mqtt_group.add_argument('--insecure', dest='mqtt_insecure', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-keepalive',
        metavar='<sec>',
        dest='mqtt_keepalive',
        type=int,
        default=DEFAULTS['mqtt-keepalive'],
        help="keepalive timeout for the client (default {})".format(DEFAULTS['mqtt-keepalive']))
    mqtt_group.add_argument('--keepalive', dest='mqtt_keepalive', type=int, help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-connect-timeout',
        metavar='<ms>',
        dest='mqtt_connect_timeout',
        type=int,
        default=DEFAULTS['mqtt-connect-timeout'],
        help="timeout for mqtt connection (default {})".format(DEFAULTS['mqtt-connect-timeout']))

    sql_group = parser.add_argument_group('SQL Options')
    sql_choices = []
    if MODULE_MYSQLDB_AVAIL:
        sql_choices.append('mysql')
    if MODULE_SQLITE3_AVAIL:
        sql_choices.append('sqlite')
    if len(sql_choices) == 0:
        SignalHandler.exitus(ExitCode.MISSING_MODULE, 'Either module MySQLdb or sqlite must be installed')

    sql_group.add_argument(
        '--sql-type',
        metavar='<type>',
        dest='sql_type',
        choices=sql_choices,
        default=DEFAULTS['sql-type'],
        help="server type {} (default '{}')".format(sql_choices, DEFAULTS['sql-type']))
    sql_group.add_argument('--sqltype', dest='sql_type', choices=sql_choices, help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-host',
        metavar='<hostname>|<socket>',
        dest='sql_host',
        default=DEFAULTS['sql-host'],
        help="hostname or unix socket to connect (default '{}')".format(DEFAULTS['sql-host']))
    sql_group.add_argument('--sqlhost', dest='sql_host', help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-port',
        metavar='<port>',
        dest='sql_port',
        type=int,
        default=DEFAULTS['sql-port'],
        help="port to connect (default {}, ignored when unix socket is defined)".format(DEFAULTS['sql-port']))
    sql_group.add_argument('--sqlport', dest='sql_port', type=int, help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-username',
        metavar='<username>',
        dest='sql_username',
        default=DEFAULTS['sql-username'],
        help="username (default {})".format(DEFAULTS['sql-username']))
    sql_group.add_argument('--sqlusername', dest='sql_username', help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-password',
        metavar='<password>',
        dest='sql_password',
        default=DEFAULTS['sql-password'],
        help="password (default {})".format(DEFAULTS['sql-password']))
    sql_group.add_argument('--sqlpassword', dest='sql_password', help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-db',
        metavar='<db>',
        dest='sql_db',
        default=DEFAULTS['sql-db'],
        help="database to use (default '{}')".format(DEFAULTS['sql-db']))
    sql_group.add_argument('--sqldb', dest='sql_db', help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-table',
        metavar='<table>',
        dest='sql_table',
        default=DEFAULTS['sql-table'],
        help="table to use (default '{}')".format(DEFAULTS['sql-table']))
    sql_group.add_argument('--sqltable', dest='sql_table', help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-max-connection',
        metavar='<num>',
        dest='sql_max_connection',
        type=int,
        default=DEFAULTS['sql-max-connection'],
        help="maximum number of simultaneous connections (default {})".format(DEFAULTS['sql-max-connection']))
    sql_group.add_argument('--sqlmaxconnection', dest='sql_max_connection', type=int, help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-connection-retry',
        metavar='<num>',
        dest='sql_connection_retry',
        type=int,
        default=DEFAULTS['sql-connection-retry'],
        help="maximum number of SQL connection retries on error (default {})".format(DEFAULTS['sql-connection-retry']))
    sql_group.add_argument(
        '--sql-connection-retry-start-delay',
        metavar='<sec>',
        dest='sql_connection_retry_start_delay',
        type=float,
        default=DEFAULTS['sql-connection-retry-start-delay'],
        help="start delay between SQL reconnect retry (default {}), is doubled after each occurrence".format(DEFAULTS['sql-connection-retry-start-delay']))
    sql_group.add_argument(
        '--sql-transaction-retry',
        metavar='<num>',
        dest='sql_transaction_retry',
        type=int,
        default=DEFAULTS['sql-transaction-retry'],
        help="maximum number of SQL transaction retry on error (default {})".format(DEFAULTS['sql-transaction-retry']))

    logging_group = parser.add_argument_group('Informational')
    logging_group.add_argument(
        '-l', '--logfile',
        metavar='<filename>',
        dest='logfile',
        default=DEFAULTS['logfile'],
        help="optional logfile (default {}".format(DEFAULTS['logfile']))
    logging_group.add_argument(
        '-d', '--debug',
        dest='debug',
        action='count',
        help='debug output')
    logging_group.add_argument(
        '-v', '--verbose',
        dest='verbose',
        action='count',
        help='verbose output')
    logging_group.add_argument(
        '-V', '--version',
        action='version',
        version='%(prog)s v'+VER)

    return parser.parse_args()

class LogLevel:
    """
    Log levels
    """
    ALWAYS = 0
    INFORMATION = 1
    NOTICE = 2
    ERROR = 3

def verbose_level():
    """
    return verbose level
    """
    global ARGS         # pylint: disable=global-statement
    return ARGS.verbose if ARGS.verbose is not None else LogLevel.ALWAYS

def debug_level():
    """
    return debug level
    """
    global ARGS         # pylint: disable=global-statement
    return ARGS.debug if ARGS.debug is not None else LogLevel.ALWAYS

def log(loglevel, msg):
    """
    Writes a message to stdout and optional logfile
    if given loglevel is >= verbose_level()

    @param msg: message to output
    """
    if verbose_level() >= loglevel:
        global ARGS   # pylint: disable=global-statement
        strtime = str(time.strftime("%Y-%m-%d %H:%M:%S"))
        # print strtime+': '+msg
        if ARGS.logfile is not None:
            filename = str(time.strftime(ARGS.logfile, time.localtime()))
            logfile = open(filename, "a")
            logfile.write(strtime+': '+msg+'\n')
            logfile.close()
        print(strtime+': '+msg)

def debuglog(dbglevel, msg):
    """
    Writes a message to stdout and optional logfile
    if given dbglevel is >= debug_level()

    @param dbglevel:
        debug level used
        e.g.
        if -d is given one time and dbglevel is 1, then msg is output
        if -d is given one time and dbglevel is 2, then msg will not output
        if -d is given two times and dbglevel is 2, then msg will output
    @param msg: message to output
    """
    if debug_level() > dbglevel:
        log(LogLevel.ALWAYS, msg)

def issocket(name):
    """
    Simple check if name is a socket name

    @param name:
        name to check

    return True if name is a socket name
    """
    nametype = re.search(r'^(\/\S*)*', name)
    return nametype is not None and nametype.group(0) == name

class Mqtt2Sql:
    """
    MQTT to SQL handling class
    """
    def __init__(self, args):
        self.exit_code = ExitCode.OK
        self.args_ = args
        self.connected = False
        self.connect_rc = 0
        self.mqtt_url = args.mqtt_url
        self.mqtt_host = args.mqtt_host
        self.mqtt_port = args.mqtt_port
        self.mqtt_username = args.mqtt_username
        self.mqtt_password = args.mqtt_password
        self.mqtt_keepalive = args.mqtt_keepalive
        self.connect_timeout = args.mqtt_connect_timeout
        self.mqtt_topic = args.mqtt_topic
        self.mqtt_exclude_topic = args.mqtt_exclude_topic
        self.cafile = args.mqtt_cafile
        self.certfile = args.mqtt_certfile
        self.keyfile = args.mqtt_keyfile
        self.insecure = args.mqtt_insecure
        self.write2sql_thread = None
        self.pool_sqlconnections = BoundedSemaphore(value=args.sql_max_connection)
        self.userdata = {
            'haveresponse' : False,
            'starttime'    : time.time()
        }
        if self.mqtt_url is not None:
            self.get_mqtt_parts()
        self.verbose_print()
        self.mqttc, ret = self.mqtt_connect()
        if ret != ExitCode.OK:
            SignalHandler.exitus(ret, '{}:{} failed - [{}] {}'.format(self.mqtt_host, self.mqtt_port, ret, mqtt.error_string(ret)))

    def write2sql(self, message):
        """
        Called as thread from mqtt backlog handler when a message has been
        received on a topic that the client subscribes to.

        @param message:
            an instance of MQTTMessage.
            This is a class with members topic, payload, qos, retain.
        """
        def sql_execute_exception(retry_condition, error_str):
            """
            handling local SQL exceptions

            @param retry_condition:
                condition for retry transaction
                if True delay process and return
                if False rollback transaction and return
            @param error_str:
                error string to output
            """
            global SQLTYPES # pylint: disable=global-statement
            nonlocal db_connection, sql, transaction_retry

            typestr = SQLTYPES[self.args_.sql_type]

            transaction_delay = random()
            transaction_delay *= 2
            debuglog(1, "[{}]: {} transaction ERROR: {}, retry={}, delay={}".format(get_ident(), typestr, error_str, transaction_retry, transaction_delay))
            if retry_condition:
                transaction_retry -= 1
                log(LogLevel.NOTICE, "SQL transaction WARNING: {} - try retry".format(error_str))
                time.sleep(transaction_delay)
            else:
                log(LogLevel.NOTICE, "{} transaction ERROR {}".format(typestr, error_str))
                log(LogLevel.ERROR, "{} give up: {}".format(typestr, sql))
                # try rollback in case there is any error
                try:
                    db_connection.rollback()
                    transaction_retry = 0
                except Exception as err:    # pylint: disable=broad-except
                    pass

        # pylint: disable=global-statement
        global SQLTYPES
        # pylint: enable=global-statement

        if self.exit_code != ExitCode.OK:
            sys.exit(0)

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        connection_retry = self.args_.sql_connection_retry
        connection_delay = self.args_.sql_connection_retry_start_delay
        connection_delay_base = self.args_.sql_connection_retry_start_delay
        debuglog(3, "SQL type is '{}'".format(SQLTYPES[self.args_.sql_type]))
        db_connection = None
        while connection_retry > 0:
            if self.exit_code != ExitCode.OK:
                sys.exit(0)
            try:
                if self.args_.sql_type == 'mysql':
                    connection = {'db': self.args_.sql_db}
                    if issocket(self.args_.sql_host):
                        connection['unix_socket'] = self.args_.sql_host
                    else:
                        connection['host'] = self.args_.sql_host
                        if self.args_.sql_port is not None:
                            connection['port'] = self.args_.sql_port
                    if self.args_.sql_username is not None:
                        connection['user'] = self.args_.sql_username
                    if self.args_.sql_password is not None:
                        connection['passwd'] = self.args_.sql_password
                    db_connection = MySQLdb.connect(**connection)

                elif self.args_.sql_type == 'sqlite':
                    db_connection = sqlite3.connect(self.args_.sql_db)
                connection_retry = 0

            except Exception as err:    # pylint: disable=broad-except
                connection_retry -= 1
                try:
                    error_code = err.args[0]
                except:     # pylint: disable=bare-except
                    error_code = 0
                if error_code == 2005:
                    connection_retry = 0
                debuglog(1, "[{}]: SQL connection ERROR: {}, retry={}, delay={}".format(get_ident(), SQLTYPES[self.args_.sql_type], connection_retry, connection_delay))
                if connection_retry > 0:
                    log(LogLevel.NOTICE, "SQL connection WARNING: {} - try retry".format(err))
                    time.sleep(connection_delay)
                    connection_delay += connection_delay_base
                else:
                    log(LogLevel.NOTICE, "SQL connection ERROR: {} - give up".format(err))
                    os.kill(os.getpid(), signal.SIGTERM)
                    SignalHandler.exitus(ExitCode.SQL_CONNECTION_ERROR, "SQL connection ERROR: {} - give up".format(err))

        transaction_retry = self.args_.sql_transaction_retry
        while transaction_retry > 0:
            if self.exit_code != ExitCode.OK:
                sys.exit(0)
            cursor = db_connection.cursor()
            try:
                # INSERT/UPDATE record
                if self.args_.sql_type == 'mysql':
                    sql = "INSERT INTO `{0}` \
                        SET `ts`='{1}',`topic`='{2}',`value`=x'{3}',`qos`='{4}',`retain`='{5}' \
                        ON DUPLICATE KEY UPDATE `ts`='{1}',`value`=x'{3}',`qos`='{4}',`retain`='{5}'"\
                        .format(
                            self.args_.sql_table,
                            timestamp,
                            message.topic,
                            message.payload.hex(),
                            message.qos,
                            message.retain
                        )
                    debuglog(4, "SQL exec: '{}'".format(sql))
                    cursor.execute(sql)
                elif self.args_.sql_type == 'sqlite':
                    sql1 = "INSERT OR IGNORE INTO `{0}` \
                            (ts,topic,value,qos,retain) \
                            VALUES('{1}','{2}',x'{3}','{4}','{5}')"\
                        .format(
                            self.args_.sql_table,
                            timestamp,
                            message.topic,
                            message.payload.hex(),
                            message.qos,
                            message.retain
                        )
                    sql2 = "UPDATE `{0}` \
                            SET ts='{1}', \
                                value=x'{3}', \
                                qos='{4}', \
                                retain='{5}' \
                            WHERE topic='{2}'"\
                        .format(
                            self.args_.sql_table,
                            timestamp,
                            message.topic,
                            message.payload.hex(),
                            message.qos,
                            message.retain
                        )
                    sql = sql1 + '\n' + sql2
                    debuglog(4, "SQL exec: '{}'".format(sql))
                    try:
                        cursor.execute(sql1)
                        cursor.execute(sql2)
                    except sqlite3.OperationalError as err:
                        self.exit_code = ExitCode.SQL_CONNECTION_ERROR
                        sys.exit(self.exit_code)

                db_connection.commit()
                debuglog(1, "[{}]: SQL success: table='{}', topic='{}', value='{}', qos='{}', retain='{}'".format(get_ident(), self.args_.sql_table, message.topic, message.payload, message.qos, message.retain))
                transaction_retry = 0

            except MySQLdb.Error as err:    # pylint: disable=no-member
                sql_execute_exception(
                    err.args[0] in [1040, 1205, 1213],
                    "[{}]: {}".format(err.args[0], err.args[1])
                    )

            except sqlite3.Error as err:
                sql_execute_exception(
                    "database is locked" in str(err).lower(),
                    err
                    )

            finally:
                cursor.close()
                db_connection.close()
                self.pool_sqlconnections.release()
                return

        db_connection.close()
        self.pool_sqlconnections.release()

    def verbose_print(self):
        """
        Verbose args
        """
        log(LogLevel.INFORMATION, '  MQTT server: {}:{} {}{} keepalive {}'.format(self.mqtt_host, self.mqtt_port, 'SSL' if (self.cafile is not None) else '', ' (suppress TLS verification)' if self.insecure else '', self.mqtt_keepalive))
        log(LogLevel.INFORMATION, '       user:   {}'.format(self.mqtt_username))
        log(LogLevel.INFORMATION, '       topics: {}'.format(self.mqtt_topic))
        log(LogLevel.INFORMATION, '       exclude: {}'.format(self.mqtt_exclude_topic))
        log(LogLevel.INFORMATION, '  SQL  type:   {}'.format(SQLTYPES[self.args_.sql_type]))
        if issocket(self.args_.sql_host):
            log(LogLevel.INFORMATION, '       server: {} [max {} connections]'.format(self.args_.sql_host, self.args_.sql_max_connection))
        else:
            log(LogLevel.INFORMATION, '       server: {}:{} [max {} connections]'.format(self.args_.sql_host, self.args_.sql_port, self.args_.sql_max_connection))
        log(LogLevel.INFORMATION, '       db:     {}'.format(self.args_.sql_db))
        log(LogLevel.INFORMATION, '       table:  {}'.format(self.args_.sql_table))
        log(LogLevel.INFORMATION, '       user:   {}'.format(self.args_.sql_username))
        if self.args_.logfile is not None:
            log(LogLevel.INFORMATION, '  Log file:    {}'.format(self.args_.logfile))
        if debug_level() > 0:
            log(LogLevel.INFORMATION, '  Debug level: {}'.format(debug_level()))
        log(LogLevel.INFORMATION, '  Verbose level: {}'.format(verbose_level()))

    def get_mqtt_parts(self):
        """
        Get mqtt connection parameter parts from url/hostnme/ip and optional arguments
        """
        try:
            URLPARSE = urllib.parse.urlparse(urllib.parse.quote(self.mqtt_url, safe='/:@'))
            if URLPARSE.netloc:
                if URLPARSE.scheme is not None:
                    self.scheme = URLPARSE.scheme
                if self.mqtt_host is None and URLPARSE.hostname is not None:
                    self.mqtt_host = urllib.parse.unquote(URLPARSE.hostname)
                if self.mqtt_port is None and URLPARSE.port is not None:
                    self.mqtt_port = URLPARSE.port
                if self.mqtt_topic is None and URLPARSE.path is not None:
                    self.mqtt_topic = urllib.parse.unquote(URLPARSE.path)[1:]
                if self.mqtt_username is None and URLPARSE.username is not None:
                    self.mqtt_username = urllib.parse.unquote(URLPARSE.username)
                if self.mqtt_password is None and URLPARSE.password is not None:
                    self.mqtt_password = urllib.parse.unquote(URLPARSE.password)
        except:     # pylint: disable=bare-except
            pass
        # set defaults if not given
        if self.scheme is None:
            self.scheme = 'mqtt'
            if self.cafile is not None or self.certfile is not None or self.keyfile is not None:
                self.scheme += 's'
        if self.mqtt_host is None:
            self.mqtt_host = DEFAULTS['mqtt-host']
        if self.mqtt_port is None:
            try:
                self.mqtt_port = DEFAULT_PORT_MQTTS if self.scheme[-1] == 's' else DEFAULT_PORT_MQTT
            except:     # pylint: disable=bare-except
                self.mqtt_port = DEFAULT_PORT_MQTT
        if self.mqtt_topic is None:
            self.mqtt_topic =  DEFAULTS['mqtt-topic']
        if self.mqtt_exclude_topic is None:
            self.mqtt_exclude_topic =  DEFAULTS['mqtt-exclude-topic']
        if self.mqtt_username is None:
            self.mqtt_username = DEFAULTS['mqtt-username']
        if self.mqtt_password is None:
            self.mqtt_password = DEFAULTS['mqtt-password']
        # disable TLS/SSL if module not available
        if not MODULE_SSL_AVAIL and len(self.scheme) and self.scheme[-1] == 's':
            log(LogLevel.INFORMATION, "Missing python SSL module - MQTT scheme '{}' not possible, use mqtt instead".format(self.scheme))
            self.scheme = 'mqtt'
            self.cafile = None
            self.certfile = None
            self.keyfile = None

    def wait_for_connect(self, connect_timeout):
        timeout = connect_timeout/10
        while not self.connected and timeout > 0:
            time.sleep(0.01)
            timeout = timeout -1

        debuglog(2, "MQTT wait_for_connect({}) returns {}".format(connect_timeout, 0 != timeout))
        return 0 != timeout

    def on_connect(self, client, userdata, message, return_code):
        """
        Called when the broker responds to our connection request.

        @param client:
            the client instance for this callback
        @param userdata:
            the private user data as set in Client() or userdata_set()
        @param message:
            response message sent by the broker
        @param return_code:
            the connection result
        """
        debuglog(1, "MQTT on_connect({},{},{},{}): {}".format(client, userdata, message, return_code, mqtt.error_string(return_code)))
        self.connected = mqtt.MQTT_ERR_SUCCESS == return_code
        self.connect_rc = return_code
        if self.connected:
            if isinstance(self.mqtt_topic, (list, tuple)):
                for topic in self.mqtt_topic:
                    debuglog(1, "subscribe to topic {}".format(topic))
                    client.subscribe(topic, 0)
            else:
                debuglog(1, "subscribe to topic {}".format(self.mqtt_topic))
                client.subscribe(self.mqtt_topic, 0)

    def on_message(self, client, userdata, message):
        """
        Called when a message has been received on a topic that the client subscribes to.
        This callback will be called for every message received.

        @param client:
            the client instance for this callback
        @param userdata:
            the private user data as set in Client() or userdata_set()
        @param message:
            an instance of MQTTMessage.
            This is a class with members topic, payload, qos, retain.
        """
        if self.exit_code != ExitCode.OK:
            sys.exit(self.exit_code)
        log(LogLevel.NOTICE, '{} {} [QOS {} Retain {}]'.format(message.topic, message.payload, message.qos, message.retain))

        debuglog(2, "on_message({},{},{})".format(client, userdata, message))

        if self.exit_code == ExitCode.OK:
            if self.mqtt_exclude_topic is not None and message.topic in self.mqtt_exclude_topic:
                return
            self.pool_sqlconnections.acquire()
            self.write2sql_thread = Thread(target=self.write2sql, args=(message,))
            self.write2sql_thread.start()

    def on_publish(self, client, userdata, mid):
        """
        Called when a message that was to be sent using the publish() call
        has completed transmission to the broker.
        For messages with QoS levels 1 and 2, this means that the appropriate
        handshakes have completed. For QoS 0, this simply means that the
        message has left the client. The mid variable matches the mid
        variable returned from the corresponding publish() call, to allow
        outgoing messages to be tracked.

        @param client:
            the client instance for this callback
        @param userdata:
            the private user data as set in Client() or userdata_set()
        @param mid:
            matches the mid variable returned from the corresponding
            publish() call, to allow outgoing messages to be tracked.
        """
        debuglog(2, "on_publish({},{},{})".format(client, userdata, mid))

    def on_subscribe(self, client, userdata, mid, granted_qos):
        """
        Called when the broker responds to a subscribe request.

        @param client:
            the client instance for this callback
        @param userdata:
            the private user data as set in Client() or userdata_set()
        @param mid:
            Matches the mid variable returned from the corresponding
            subscribe() call.
        @param granted_qos:
            a list of integers that give the QoS level the broker has
            granted for each of the different subscription requests.
        """
        debuglog(2, "on_subscribe({},{},{},{})".format(client, userdata, mid, granted_qos))

    def on_log(self, client, userdata, level, string):
        """
        Called when the client has log information.

        @param client:
            the client instance for this callback
        @param userdata:
            the private user data as set in Client() or userdata_set()
        @param level:
            gives the severity of the message and will be one of
            MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR,
            and MQTT_LOG_DEBUG. The message itself is in string.
        @param string:
            The message itself
        """
        debuglog(2, "on_log({},{},{},{})".format(client, userdata, level, string))

    def mqtt_connect(self):
        """
        Create MQTT client and set callback handler

        @returns
            mqttc:  MQTT client handle (or None on error)
            ret:    Error code
        """
        mqttc = mqtt.Client('{}-{:d}'.format(SCRIPTNAME, os.getpid()), clean_session=True, userdata=self.userdata, protocol=mqtt.MQTTv31)
        if debug_level() > 0:
            if debug_level() <= 2:
                logging.basicConfig(level=logging.WARNING)
            elif debug_level() <= 3:
                logging.basicConfig(level=logging.INFO)
            elif debug_level() >= 4:
                logging.basicConfig(level=logging.DEBUG)
            logger = logging.getLogger(__name__)
            mqttc.enable_logger(logger)
        mqttc.on_connect = self.on_connect
        mqttc.on_message = self.on_message
        mqttc.on_publish = self.on_publish
        mqttc.on_subscribe = self.on_subscribe
        if debug_level() > 0:
            mqttc.on_log = self.on_log

        # cafile controls TLS usage
        if self.cafile is not None or self.certfile is not None or self.keyfile is not None:
            mqttc.tls_set(ca_certs=self.cafile,
                          certfile=self.certfile,
                          keyfile=self.keyfile,
                          cert_reqs=ssl.CERT_REQUIRED)
            mqttc.tls_insecure_set(self.insecure)

        # username & password may be None
        if self.mqtt_username is not None:
            mqttc.username_pw_set(self.mqtt_username, self.mqtt_password)

        debuglog(1, "mqttc.connect({}, {}, {})".format(self.mqtt_host, self.mqtt_port, self.mqtt_keepalive))
        try:
            res = mqttc.connect(self.mqtt_host, self.mqtt_port, self.mqtt_keepalive)
            debuglog(1, "mqttc.connect() returns {}".format(res))
        except Exception as err:    # pylint: disable=broad-except,unused-variable
            return None, ExitCode.MQTT_CONNECTION_ERROR

        mqttc.loop_start()      # Start loop to process on_connect()
        if not self.wait_for_connect(self.connect_timeout):
            return None, self.connect_rc
        mqttc.loop_stop()       # Stop loop

        return mqttc, ExitCode.OK

    def loop_forever(self):
        """
        Main MQTT to SQL loop
        does not return until an error occurs
        """
        while True:
            # Main loop as long as no error occurs
            ret = mqtt.MQTT_ERR_SUCCESS
            while not self.userdata['haveresponse'] and ret == mqtt.MQTT_ERR_SUCCESS:
                try:
                    ret = self.mqttc.loop()
                except Exception as err:    # pylint: disable=broad-except
                    log(LogLevel.ERROR, 'ERROR: loop() - {}'.format(err))
                    time.sleep(0.1)
                if self.exit_code != ExitCode.OK:
                    sys.exit(self.exit_code)
            if ret == mqtt.MQTT_ERR_CONN_LOST:
                # disconnect from server
                log(LogLevel.NOTICE, 'Remote disconnected from MQTT - [{}] {})'.format(ret, mqtt.error_string(ret)))
                try:
                    ret = self.mqttc.reconnect()
                    log(LogLevel.NOTICE, 'MQTT reconnected - [{}] {}'.format(ret, mqtt.error_string(ret)))
                except Exception as err:    # pylint: disable=broad-except
                    SignalHandler.exitus(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed - [{}] {}'.format(self.mqtt_host, self.mqtt_port, ret, mqtt.error_string(err)))
            else:
                SignalHandler.exitus(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed: - [{}] {}'.format(self.mqtt_host, self.mqtt_port, ret, mqtt.error_string(ret)))

class SignalHandler:
    """
    Signal Handler Class
    """
    def __init__(self):
        signal.signal(signal.SIGINT, self._exitus)
        signal.signal(signal.SIGTERM, self._exitus)

    def _signalname(self, signal_):
        signals_ = dict((k, v) for v, k in reversed(sorted(signal.__dict__.items())) if v.startswith('SIG') and not v.startswith('SIG_'))
        return signals_.get(signal_, 'UNKNOWN')

    def _exitus(self, signal_, frame_):   # pylint: disable=unused-argument
        """
        local exit with logging
        """
        self.exitus(signal_, 'signal {}#{}'.format(self._signalname(signal_), signal_))

    def exitus(self, status=0, message="end"):
        """
        Called when the program should be exit

        @param status:
            the exit status program returns to callert
        @param message:
            the message logged before exit
        """
        # pylint: disable=global-statement
        global SCRIPTNAME
        global SCRIPTPID
        global VER
        # pylint: enable=global-statement
        if message is not None:
            log(LogLevel.INFORMATION, message)
        log(LogLevel.INFORMATION, '{}[{}] v{} end'.format(SCRIPTNAME, SCRIPTPID, VER))
        if status in (signal.SIGINT, signal.SIGTERM):
            status = 0
        sys.exit(status)

if __name__ == "__main__":
    # set signal handler
    SIG = SignalHandler()

    # Parse command line arguments
    ARGS = parseargs()

    # Log program start
    log(LogLevel.INFORMATION, '{}[{}] v{} start'.format(SCRIPTNAME, SCRIPTPID, VER))

    # Create class
    MQTT2SQL = Mqtt2Sql(ARGS)
    # run loop
    MQTT2SQL.loop_forever()
