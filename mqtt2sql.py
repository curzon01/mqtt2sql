#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
VER = '2.4.0039'

"""
    mqtt2mysql.py - Copy MQTT topic payloads to MySQL/SQLite database

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


Requirements:
    Python 3, Paho MQTT, MySQLdb:
    sudo apt-get install python3 python3-pip python3-mysqldb python3-configargparse python3-paho-mqtt

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
EXIT_CODE = ExitCode.OK
SQLTYPES = {'mysql':'MySQl', 'sqlite':'SQLite'}
ARGS = {}
DEFAULTS = {
    'configfile': None,
    'logfile': None,
    'debug': None,
    'verbose': None,

    'mqtt-host': 'localhost',
    'mqtt-port': 1883,
    'mqtt-username': None,
    'mqtt-password': None,
    'mqtt-topic': '#',
    'mqtt-cafile': None,
    'mqtt-certfile': None,
    'mqtt-keyfile': None,
    'mqtt-insecure': False,
    'mqtt-keepalive': 60,

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

    parser = configargparse.ArgumentParser(\
            description='MQTT to MySQL bridge. Subscribes to MQTT broker topic(s) and write values into a database.',
            epilog="There are no required arguments, defaults are displayed using --help."
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
        '--mqtt-host',
        metavar='<host>',
        dest='mqtt_host',
        default=DEFAULTS['mqtt-host'],
        help="host to connect to (default '{}')".format(DEFAULTS['mqtt-host'])
    )
    mqtt_group.add_argument('--mqtthost', '--host', dest='mqtt_host', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-port',
        metavar='<port>',
        dest='mqtt_port',
        type=int,
        default=DEFAULTS['mqtt-port'],
        help="port to connect to (default {})".format(DEFAULTS['mqtt-port'])
    )
    mqtt_group.add_argument('--mqttport', '--port', dest='mqtt_port', type=int, help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-username',
        metavar='<username>',
        dest='mqtt_username',
        default=DEFAULTS['mqtt-username'],
        help="username (default {})".format(DEFAULTS['mqtt-username'])
    )
    mqtt_group.add_argument('--mqttusername', '--username', dest='mqtt_username', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-password',
        metavar='<password>',
        dest='mqtt_password',
        default=DEFAULTS['mqtt-password'],
        help="password (default {})".format(DEFAULTS['mqtt-password']))
    mqtt_group.add_argument('--mqttpassword', '--password', dest='mqtt_password', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-topic',
        metavar='<topic>',
        dest='mqtt_topic',
        nargs='*',
        default=DEFAULTS['mqtt-topic'],
        help="topic to use (default {})".format(DEFAULTS['mqtt-topic']))
    mqtt_group.add_argument('--topic', dest='mqtt_topic', nargs='*', help=configargparse.SUPPRESS)
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

def verbose_print(_args):
    """
    Verbose args

    @param _args:
        result from configargparse.parse_args()
    """
    log(1, '  MQTT server: {}:{} {}{} keepalive {}'.format(_args.mqtt_host, _args.mqtt_port, 'SSL' if (_args.mqtt_cafile is not None) else '', ' (suppress TLS verification)' if _args.mqtt_insecure else '', _args.mqtt_keepalive))
    log(1, '       user:   {}'.format(_args.mqtt_username))
    log(1, '       topics: {}'.format(_args.mqtt_topic))
    log(1, '  SQL  type:   {}'.format(SQLTYPES[_args.sql_type]))
    log(1, '       server: {}:{} [max {} connections]'.format(_args.sql_host, _args.sql_port, _args.sql_max_connection))
    log(1, '       db:     {}'.format(_args.sql_db))
    log(1, '       table:  {}'.format(_args.sql_table))
    log(1, '       user:   {}'.format(_args.sql_username))
    if _args.logfile is not None:
        log(1, '  Log file:    {}'.format(_args.logfile))
    if debug_level() > 0:
        log(1, '  Debug level: {}'.format(debug_level()))
    log(1, '  Verbose level: {}'.format(verbose_level()))

def verbose_level():
    """
    return verbose level (0 if disabled)
    """
    global ARGS         # pylint: disable=global-statement
    if ARGS.verbose is not None and ARGS.verbose > 0:
        return ARGS.verbose
    return 0

def debug_level():
    """
    return debug level (0 if disabled)
    """
    global ARGS         # pylint: disable=global-statement
    if ARGS.debug is not None and ARGS.debug > 0:
        return ARGS.debug
    return 0

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
        log(0, msg)

class Mqtt2Sql:
    """
    MQTT to SQL handling class
    """
    def __init__(self, args_):
        self._args = args_
        self.write2sql_thread = None
        self.pool_sqlconnections = BoundedSemaphore(value=self._args.sql_max_connection)
        self.userdata = {
            'haveresponse' : False,
            'starttime'    : time.time()
        }
        self.mqttc, ret = self.mqtt_connect(
            host=self._args.mqtt_host,
            port=self._args.mqtt_port,
            username=self._args.mqtt_username,
            password=self._args.mqtt_password,
            keepalive=self._args.mqtt_keepalive,
            cafile=self._args.mqtt_cafile,
            certfile=self._args.mqtt_certfile,
            keyfile=self._args.mqtt_keyfile,
            insecure=self._args.mqtt_insecure,
            userdata=self.userdata
            )
        if ret != ExitCode.OK:
            SignalHandler.exitus(ret, '{}:{} failed - [{}] {} - "{}"'.format(self._args.mqtt_host, self._args.mqtt_port, ret, mqtt.error_string(ret), err))

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

            typestr = SQLTYPES[self._args.sql_type]

            transaction_delay = random()
            transaction_delay *= 2
            debuglog(1, "[{}]: {} transaction ERROR: {}, retry={}, delay={}".format(get_ident(), typestr, error_str, transaction_retry, transaction_delay))
            if retry_condition:
                transaction_retry -= 1
                log(1, "SQL transaction NOTE: {} - try retry".format(err))
                time.sleep(transaction_delay)
            else:
                log(0, "{} transaction ERROR {}".format(typestr, error_str))
                log(1, "{} give up: {}".format(typestr, sql))
                # Rollback in case there is any error
                db_connection.rollback()
                transaction_retry = 0

        # pylint: disable=global-statement
        global EXIT_CODE
        global SQLTYPES
        # pylint: enable=global-statement

        if EXIT_CODE != ExitCode.OK:
            sys.exit(0)

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        connection_retry = self._args.sql_connection_retry
        connection_delay = self._args.sql_connection_retry_start_delay
        connection_delay_base = self._args.sql_connection_retry_start_delay
        debuglog(3, "SQL type is '{}'".format(SQLTYPES[self._args.sql_type]))
        db_connection = None
        while connection_retry > 0:
            if EXIT_CODE != ExitCode.OK:
                sys.exit(0)
            try:
                if self._args.sql_type == 'mysql':
                    connection = {'db': self._args.sql_db}
                    # test for unix socket name
                    hosttype = re.search(r'^(\/\S*)*', self._args.sql_host)
                    if hosttype is not None and hosttype.group(0) == self._args.sql_host:
                        connection['unix_socket'] = self._args.sql_host
                    else:
                        connection['host'] = self._args.sql_host
                        if self._args.sql_port is not None:
                            connection['port'] = self._args.sql_port
                    if self._args.sql_username is not None:
                        connection['user'] = self._args.sql_username
                    if self._args.sql_password is not None:
                        connection['passwd'] = self._args.sql_password
                    db_connection = MySQLdb.connect(**connection)
                elif self._args.sql_type == 'sqlite':
                    db_connection = sqlite3.connect(self._args.sql_db)
                connection_retry = 0
            except Exception as err:    # pylint: disable=broad-except
                connection_retry -= 1
                try:
                    error_code = err.args[0]
                except:     # pylint: disable=bare-except
                    error_code = 0
                if error_code == 2005:
                    connection_retry = 0
                debuglog(1, "[{}]: SQL connection ERROR: {}, retry={}, delay={}".format(get_ident(), SQLTYPES[self._args.sql_type], connection_retry, connection_delay))
                if connection_retry > 0:
                    log(1, "SQL connection NOTE: {} - try retry".format(err))
                    time.sleep(connection_delay)
                    connection_delay += connection_delay_base
                else:
                    log(1, "SQL connection ERROR: {} - give up".format(err))
                    os.kill(os.getpid(), signal.SIGTERM)
                    SignalHandler.exitus(ExitCode.SQL_CONNECTION_ERROR, "SQL connection ERROR: {} - give up".format(err))

        transaction_retry = self._args.sql_transaction_retry
        while transaction_retry > 0:
            if EXIT_CODE != ExitCode.OK:
                sys.exit(0)
            cursor = db_connection.cursor()
            try:
                # INSERT/UPDATE record
                payload = message.payload
                if not isinstance(payload, str):
                    payload = str(payload, 'utf-8', errors='ignore')
                if self._args.sql_type == 'mysql':
                    sql = "INSERT INTO `{0}` \
                        SET `ts`='{1}',`topic`='{2}',`value`='{3}',`qos`='{4}',`retain`='{5}' \
                        ON DUPLICATE KEY UPDATE `ts`='{1}',`value`='{3}',`qos`='{4}',`retain`='{5}'"\
                        .format(
                            self._args.sql_table,
                            timestamp,
                            message.topic,
                            payload,
                            message.qos,
                            message.retain
                        )
                    debuglog(4, "SQL exec: '{}'".format(sql))
                    cursor.execute(sql)
                elif self._args.sql_type == 'sqlite':
                    # strtime=str(time.strftime("%Y-%m-%d %H:%M:%S"))
                    sql1 = "INSERT OR IGNORE INTO `{0}` \
                            (ts,topic,value,qos,retain) \
                            VALUES('{1}','{2}','{3}','{4}','{5}')"\
                        .format(
                            self._args.sql_table,
                            timestamp,
                            message.topic,
                            payload,
                            message.qos,
                            message.retain
                        )
                    sql2 = "UPDATE `{0}` \
                            SET ts='{1}', \
                                value='{3}', \
                                qos='{4}', \
                                retain='{5}' \
                            WHERE topic='{2}'"\
                        .format(
                            self._args.sql_table,
                            timestamp,
                            message.topic,
                            payload,
                            message.qos,
                            message.retain
                        )
                    sql = sql1 + '\n' + sql2
                    debuglog(4, "SQL exec: '{}'".format(sql))
                    try:
                        cursor.execute(sql1)
                        cursor.execute(sql2)
                    except sqlite3.OperationalError as err:
                        EXIT_CODE = ExitCode.SQL_CONNECTION_ERROR
                        sys.exit(EXIT_CODE)

                db_connection.commit()
                debuglog(1, "[{}]: SQL success: table='{}', topic='{}', value='{}', qos='{}', retain='{}'".format(get_ident(), self._args.sql_table, message.topic, payload, message.qos, message.retain))
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
        for topic in self._args.mqtt_topic:
            debuglog(1, "subscribe to topic {}".format(topic))
            client.subscribe(topic, 0)

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
        global EXIT_CODE    # pylint: disable=global-statement

        if EXIT_CODE != ExitCode.OK:
            sys.exit(EXIT_CODE)
        log(2, '{} {} [QOS {} Retain {}]'.format(message.topic, message.payload, message.qos, message.retain))

        debuglog(2, "on_message({},{},{})".format(client, userdata, message))
        if EXIT_CODE == ExitCode.OK:
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

    def mqtt_connect(self, host, port, username=None, password=None, keepalive=60, cafile=None, certfile=None, keyfile=None, insecure=True, userdata=None):
        """
        Create MQTT client and set callback handler

        @param host:
            the hostname or IP address of the remote broker
        @param port:
            the network port of the server host to connect to
        @param username:
            username for broker authentication
        @param password:
            password for broker authentication
        @param keepalive:
            maximum period in seconds allowed between communications with the broker
        @param cafile:
            a string path to the Certificate Authority certificate files that are to be treated as trusted by this client
        @param certfile:
            strings pointing to the PEM encoded client certificate
        @param keyfile:
            strings pointing to the PEM encoded private keys
        @param insecure:
            verification of the server hostname in the server certificate if True
        @param userdata:
            private user data that will be passed to callbacks

        @returns
            mqttc:  MQTT client handle (or None on error)
            ret:    Error code
        """
        mqttc = mqtt.Client('{}-{:d}'.format(SCRIPTNAME, os.getpid()), clean_session=True, userdata=userdata)
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
        if cafile is not None:
            if certfile is not None and keyfile is not None:
                mqttc.tls_set(cafile,
                              certfile=certfile,
                              keyfile=keyfile,
                              cert_reqs=ssl.CERT_REQUIRED)
            else:
                mqttc.tls_set(cafile, cert_reqs=ssl.CERT_REQUIRED)
            mqttc.tls_insecure_set(insecure)

        # username & password may be None
        if username is not None:
            mqttc.username_pw_set(username, password)

        debuglog(1, "mqttc.connect({}, {}, {})".format(host, port, keepalive))
        try:
            res = mqttc.connect(host, port, keepalive)
            debuglog(1, "mqttc.connect() returns {}".format(res))
        except Exception as err:    # pylint: disable=broad-except,unused-variable
            return None, ExitCode.MQTT_CONNECTION_ERROR

        return mqttc, ExitCode.OK

    def loop_forever(self):
        """
        Main MQTT to SQL loop
        does not return until an error occurs
        """
        global EXIT_CODE    # pylint: disable=global-statement

        while True:
            # Main loop as long as no error occurs
            ret = mqtt.MQTT_ERR_SUCCESS
            while not self.userdata['haveresponse'] and ret == mqtt.MQTT_ERR_SUCCESS:
                try:
                    ret = self.mqttc.loop()
                except Exception as err:    # pylint: disable=broad-except
                    log(0, 'ERROR: loop() - {}'.format(err))
                    time.sleep(0.1)
                if EXIT_CODE != ExitCode.OK:
                    sys.exit(EXIT_CODE)
            if ret not in (
                    mqtt.MQTT_ERR_AGAIN,
                    mqtt.MQTT_ERR_PROTOCOL,
                    mqtt.MQTT_ERR_INVAL,
                    mqtt.MQTT_ERR_NO_CONN,
                    mqtt.MQTT_ERR_CONN_REFUSED,
                    mqtt.MQTT_ERR_NOT_FOUND,
                    mqtt.MQTT_ERR_TLS,
                    mqtt.MQTT_ERR_PAYLOAD_SIZE,
                    mqtt.MQTT_ERR_NOT_SUPPORTED,
                    mqtt.MQTT_ERR_AUTH,
                    mqtt.MQTT_ERR_ERRNO):
                # disconnect from server
                log(0, 'Remote disconnected from MQTT - [{}] {})'.format(ret, mqtt.error_string(ret)))
                try:
                    ret = self.mqttc.reconnect()
                    log(0, 'MQTT reconnected - [{}] {})'.format(ret, mqtt.error_string(ret)))
                except Exception as err:    # pylint: disable=broad-except
                    SignalHandler.exitus(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed - [{}] {}'.format(self._args.mqtt_host, self._args.mqtt_port, ret, mqtt.error_string(err)))
            else:
                SignalHandler.exitus(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed: - [{}] {}'.format(self._args.mqtt_host, self._args.mqtt_port, ret, mqtt.error_string(ret)))

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
            log(1, message)
        log(0, '{}[{}] v{} end'.format(SCRIPTNAME, SCRIPTPID, VER))
        if status in (signal.SIGINT, signal.SIGTERM):
            status = 0
        sys.exit(status)

if __name__ == "__main__":
    # set signal handler
    SIG = SignalHandler()

    # Parse command line arguments
    ARGS = parseargs()

    # Log program start
    log(0, '{}[{}] v{} start'.format(SCRIPTNAME, SCRIPTPID, VER))

    # print possible verbose info
    verbose_print(ARGS)

    # Create class
    MQTT2SQL = Mqtt2Sql(ARGS)
    # run loop
    MQTT2SQL.loop_forever()
