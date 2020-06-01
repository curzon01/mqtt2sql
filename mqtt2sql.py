#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
VER = '2.1.0033'

"""
    mqtt2mysql.py - Copy MQTT topic payloads to MySQL/SQLite database

    Copyright (C) 2019 Norbert Richter <nr@prsolution.eu>

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
    import threading
    from threading import Thread, BoundedSemaphore
    from random import random
except ImportError as err:
    module_import_error(err)

try:
    import ssl
    MODULE_SSL = True
except ImportError:
    MODULE_SSL = False

try:
    import MySQLdb
    MODULE_MYSQLDB = True
except ImportError:
    MODULE_MYSQLDB = False

try:
    import sqlite3
    MODULE_SQLITE3 = True
except ImportError:
    MODULE_SQLITE3 = False
# pylint: enable=wrong-import-position

SCRIPTNAME = os.path.basename(sys.argv[0])
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

    'sql-type': 'mysql' if MODULE_MYSQLDB else ('sqlite' if MODULE_SQLITE3 else None),
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

def log(msg):
    """
    Writes a message to stdout and optional logfile

    @param msg: message to output
    """
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
    if given dbglevel is >= ARGS.debug

    @param dbglevel:
        debug level used
        e.g.
        if -d is given one time and dbglevel is 1, then msg is output
        if -d is given one time and dbglevel is 2, then msg will not output
        if -d is given two times and dbglevel is 2, then msg will output
    @param msg: message to output
    """
    global ARGS   # pylint: disable=global-statement
    if ARGS.debug is not None and ARGS.debug > dbglevel:
        log(msg)

def write2sql(message):
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
        # pylint: disable=global-statement
        global ARGS
        global SQLTYPES
        # pylint: enable=global-statement
        nonlocal db_connection, sql, transaction_retry

        typestr = SQLTYPES[ARGS.sql_type]

        transaction_delay = random()
        transaction_delay *= 2
        debuglog(1, "[{}]: {} transaction ERROR: {}, retry={}, delay={}".format(threading.get_ident(), typestr, error_str, transaction_retry, transaction_delay))
        if retry_condition:
            transaction_retry -= 1
            log("SQL transaction ERROR: {} - try retry".format(err))
            time.sleep(transaction_delay)
        else:
            log("{} transaction ERROR {}".format(typestr, error_str))
            log("{} give up: {}".format(typestr, sql))
            # Rollback in case there is any error
            db_connection.rollback()
            transaction_retry = 0

    # pylint: disable=global-statement
    global EXIT_CODE
    global ARGS
    global SQLTYPES
    global POOL_SQLCONNECTIONS
    # pylint: enable=global-statement

    if EXIT_CODE != ExitCode.OK:
        sys.exit(0)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    connection_retry = ARGS.sql_connection_retry
    connection_delay = ARGS.sql_connection_retry_start_delay
    connection_delay_base = ARGS.sql_connection_retry_start_delay
    debuglog(3, "SQL type is '{}'".format(SQLTYPES[ARGS.sql_type]))
    db_connection = None
    while connection_retry > 0:
        if EXIT_CODE != ExitCode.OK:
            sys.exit(0)
        try:
            if ARGS.sql_type == 'mysql':
                if ARGS.sql_username is not None and ARGS.sql_password is not None:
                    db_connection = MySQLdb.connect(ARGS.sql_host, ARGS.sql_username, ARGS.sql_password, ARGS.sql_db)
                else:
                    db_connection = MySQLdb.connect(ARGS.sql_host)
            elif ARGS.sql_type == 'sqlite':
                db_connection = sqlite3.connect(ARGS.sql_db)
            connection_retry = 0

        except Exception as err:    # pylint: disable=broad-except
            connection_retry -= 1
            debuglog(1, "[{}]: SQL connection ERROR: {}, retry={}, delay={}".format(threading.get_ident(), SQLTYPES[ARGS.sql_type], connection_retry, connection_delay))
            if connection_retry > 0:
                log("SQL connection ERROR: {} - try retry".format(err))
                time.sleep(connection_delay)
                connection_delay += connection_delay_base
            else:
                os.kill(os.getpid(), signal.SIGTERM)
                exit_(ExitCode.SQL_CONNECTION_ERROR, "SQL connection ERROR: {} - give up".format(err))

    transaction_retry = ARGS.sql_transaction_retry
    while transaction_retry > 0:
        if EXIT_CODE != ExitCode.OK:
            sys.exit(0)
        cursor = db_connection.cursor()
        try:
            # INSERT/UPDATE record
            payload = message.payload
            if not isinstance(payload, str):
                payload = str(payload, 'utf-8', errors='ignore')
            if ARGS.sql_type == 'mysql':
                sql = "INSERT INTO `{0}` \
                       SET `ts`='{1}',`topic`='{2}',`value`='{3}',`qos`='{4}',`retain`='{5}' \
                       ON DUPLICATE KEY UPDATE `ts`='{1}',`value`='{3}',`qos`='{4}',`retain`='{5}'"\
                    .format(
                        ARGS.sql_table,
                        timestamp,
                        message.topic,
                        payload,
                        message.qos,
                        message.retain
                    )
                debuglog(4, "SQL exec: '{}'".format(sql))
                cursor.execute(sql)
            elif ARGS.sql_type == 'sqlite':
                # strtime=str(time.strftime("%Y-%m-%d %H:%M:%S"))
                sql1 = "INSERT OR IGNORE INTO `{0}` \
                        (ts,topic,value,qos,retain) \
                        VALUES('{1}','{2}','{3}','{4}','{5}')"\
                    .format(
                        ARGS.sql_table,
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
                        ARGS.sql_table,
                        timestamp,
                        message.topic,
                        payload,
                        message.qos,
                        message.retain
                    )
                sql = sql1 + '\n' + sql2
                debuglog(4, "SQL exec: '{}'".format(sql))
                cursor.execute(sql1)
                cursor.execute(sql2)

            db_connection.commit()
            debuglog(1, "[{}]: SQL success: table='{}', topic='{}', value='{}', qos='{}', retain='{}'".format(threading.get_ident(), ARGS.sql_table, message.topic, payload, message.qos, message.retain))
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
    POOL_SQLCONNECTIONS.release()


def on_connect(client, userdata, message, return_code):
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
    global ARGS   # pylint: disable=global-statement
    debuglog(1, "MQTT on_connect({},{},{},{}): {}".format(client, userdata, message, return_code, mqtt.error_string(return_code)))
    for topic in ARGS.mqtt_topic:
        debuglog(1, "subscribe to topic {}".format(topic))
        client.subscribe(topic, 0)

def on_message(client, userdata, message):
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
    # pylint: disable=global-statement
    global EXIT_CODE
    global ARGS
    global POOL_SQLCONNECTIONS
    # pylint: enable=global-statement
    if EXIT_CODE != ExitCode.OK:
        sys.exit(EXIT_CODE)
    if ARGS.verbose is not None and ARGS.verbose > 0:
        log('{} {} [QOS {} Retain {}]'.format(message.topic, message.payload, message.qos, message.retain))

    debuglog(2, "on_message({},{},{})".format(client, userdata, message))
    POOL_SQLCONNECTIONS.acquire()
    Thread(target=write2sql, args=(message,)).start()

def on_publish(client, userdata, mid):
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

def on_subscribe(client, userdata, mid, granted_qos):
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

def on_log(client, userdata, level, string):
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

def exit_(status=0, message="end"):
    """
    Called when the program should be exit

    @param status:
        the exit status program returns to callert
    @param message:
        the message logged before exit
    """
    global EXIT_CODE    # pylint: disable=global-statement

    log(message)
    EXIT_CODE = status
    sys.exit(status)

class SignalHandler:
    """
    Signal Handler Class
    """
    def __init__(self):
        signal.signal(signal.SIGINT, self.exitus)
        signal.signal(signal.SIGTERM, self.exitus)

    def exitus(self, signal_, frame_):
        """
        local exit with logging
        """
        global SCRIPTNAME   # pylint: disable=global-statement
        debuglog(2, "SignalHandler.exitus({},{})".format(signal_, frame_))
        exit_(signal, '{} v{} end - signal {}'.format(SCRIPTNAME, VER, signal_))

def parseargs():
    """
    Program argument parser

    @return:
        configargparse.parse_args() result
    """
    # pylint: disable=global-statement
    global VER
    global DEFAULTS
    global MODULE_SSL
    global MODULE_MYSQLDB
    global MODULE_SQLITE3
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
        help="cafile (default {})".format(DEFAULTS['mqtt-cafile']) if MODULE_SSL else configargparse.SUPPRESS)
    mqtt_group.add_argument('--cafile', dest='mqtt_cafile', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-certfile',
        metavar='<certfile>',
        dest='mqtt_certfile',
        default=DEFAULTS['mqtt-certfile'],
        help="certfile (default {})".format(DEFAULTS['mqtt-certfile']) if MODULE_SSL else configargparse.SUPPRESS)
    mqtt_group.add_argument('--certfile', dest='mqtt_certfile', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-keyfile',
        metavar='<keyfile>',
        dest='mqtt_keyfile',
        default=DEFAULTS['mqtt-keyfile'],
        help="keyfile (default {})".format(DEFAULTS['mqtt-keyfile']) if MODULE_SSL else configargparse.SUPPRESS)
    mqtt_group.add_argument('--keyfile', dest='mqtt_keyfile', help=configargparse.SUPPRESS)
    mqtt_group.add_argument(
        '--mqtt-insecure',
        dest='mqtt_insecure',
        action='store_true',
        default=DEFAULTS['mqtt-insecure'],
        help="suppress TLS verification (default {})".format(DEFAULTS['mqtt-insecure']) if MODULE_SSL else configargparse.SUPPRESS)
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
    if MODULE_MYSQLDB:
        sql_choices.append('mysql')
    if MODULE_SQLITE3:
        sql_choices.append('sqlite')
    if len(sql_choices) == 0:
        exit_(ExitCode.MISSING_MODULE, 'Either module MySQLdb or sqlite must be installed')

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
        metavar='<host>',
        dest='sql_host',
        default=DEFAULTS['sql-host'],
        help="host to connect (default '{}')".format(DEFAULTS['sql-host']))
    sql_group.add_argument('--sqlhost', dest='sql_host', help=configargparse.SUPPRESS)
    sql_group.add_argument(
        '--sql-port',
        metavar='<port>',
        dest='sql_port',
        type=int,
        default=DEFAULTS['sql-port'],
        help="port to connect (default {})".format(DEFAULTS['sql-port']))
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

if __name__ == "__main__":
    # set signal handler
    SIG = SignalHandler()

    # Parse command line arguments
    ARGS = parseargs()

    # Log program start
    log('{} v{} start'.format(SCRIPTNAME, VER))

    if ARGS.verbose is not None and ARGS.verbose > 0:
        log('  MQTT server: {}:{} {}{} keepalive {}'.format(ARGS.mqtt_host, ARGS.mqtt_port, 'SSL' if (ARGS.mqtt_cafile is not None) else '', ' (suppress TLS verification)' if ARGS.mqtt_insecure else '', ARGS.mqtt_keepalive))
        log('       user:   {}'.format(ARGS.mqtt_username))
        log('       topics: {}'.format(ARGS.mqtt_topic))
        log('  SQL  type:   {}'.format(SQLTYPES[ARGS.sql_type]))
        log('       server: {}:{} [max {} connections]'.format(ARGS.sql_host, ARGS.sql_port, ARGS.sql_max_connection))
        log('       db:     {}'.format(ARGS.sql_db))
        log('       table:  {}'.format(ARGS.sql_table))
        log('       user:   {}'.format(ARGS.sql_username))
        if ARGS.logfile is not None:
            log('  Log file:    {}'.format(ARGS.logfile))
        if ARGS.debug is not None and ARGS.debug > 0:
            log('  Debug level: {}'.format(ARGS.debug))
        if ARGS.verbose is not None and ARGS.verbose > 0:
            log('  Verbose level: {}'.format(ARGS.verbose))

    POOL_SQLCONNECTIONS = BoundedSemaphore(value=ARGS.sql_max_connection)

    # Create MQTT client and set callback handler
    USERDATA = {
        'haveresponse' : False,
        'starttime'    : time.time(),
    }
    MQTTC = mqtt.Client('{}-{:d}'.format(SCRIPTNAME, os.getpid()), clean_session=True, userdata=USERDATA)
    if ARGS.debug is not None and ARGS.debug > 0:
        if ARGS.debug == 1:
            logging.basicConfig(level=logging.INFO)
        elif ARGS.debug == 2:
            logging.basicConfig(level=logging.WARNING)
        elif ARGS.debug == 3:
            logging.basicConfig(level=logging.DEBUG)
        elif ARGS.debug >= 4:
            logging.basicConfig(level=logging.DEBUG)
        LOGGER = logging.getLogger(__name__)
        MQTTC.enable_logger(LOGGER)
    MQTTC.on_connect = on_connect
    MQTTC.on_message = on_message
    MQTTC.on_publish = on_publish
    MQTTC.on_subscribe = on_subscribe
    MQTTC.on_log = on_log

    # cafile controls TLS usage
    if ARGS.mqtt_cafile is not None:
        if ARGS.mqtt_certfile is not None:
            MQTTC.tls_set(
                ca_certs=ARGS.mqtt_cafile,
                certfile=ARGS.mqtt_certfile,
                keyfile=ARGS.mqtt_keyfile,
                cert_reqs=ssl.CERT_REQUIRED
            )
        else:
            MQTTC.tls_set(ARGS.mqtt_cafile, cert_reqs=ssl.CERT_REQUIRED)
        MQTTC.tls_insecure_set(ARGS.mqtt_insecure)

    # username & password may be None
    if ARGS.mqtt_username is not None:
        MQTTC.username_pw_set(ARGS.mqtt_username, ARGS.mqtt_password)

    # Attempt to connect to broker. If this fails, issue CRITICAL
    debuglog(1, "MQTTC.connect({}, {}, {})".format(ARGS.mqtt_host, ARGS.mqtt_port, ARGS.mqtt_keepalive))
    RETURN_CODE = mqtt.MQTT_ERR_UNKNOWN
    try:
        RETURN_CODE = MQTTC.connect(ARGS.mqtt_host, ARGS.mqtt_port, ARGS.mqtt_keepalive)
        debuglog(1, "MQTTC.connect() returns {}".format(RETURN_CODE))
    except Exception as err:    # pylint: disable=broad-except
        exit_(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed - [{}] {} - "{}"'.format(ARGS.mqtt_host, ARGS.mqtt_port, RETURN_CODE, mqtt.error_string(RETURN_CODE), err))

    while True:
        # Main loop as long as no error occurs
        RETURN_CODE = mqtt.MQTT_ERR_SUCCESS
        while not USERDATA['haveresponse'] and RETURN_CODE == mqtt.MQTT_ERR_SUCCESS:
            try:
                RETURN_CODE = MQTTC.loop()
            except Exception as err:    # pylint: disable=broad-except
                log('ERROR: loop() - {}'.format(err))
                time.sleep(0.25)
            if EXIT_CODE != ExitCode.OK:
                sys.exit(EXIT_CODE)
        if RETURN_CODE not in (
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
            log('MQTT disconnected - [{}] {})'.format(RETURN_CODE, mqtt.error_string(RETURN_CODE)))
            try:
                RETURN_CODE = MQTTC.reconnect()
                log('MQTT reconnected - [{}] {})'.format(RETURN_CODE, mqtt.error_string(RETURN_CODE)))
            except Exception as err:    # pylint: disable=broad-except
                exit_(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed - [{}] {}'.format(ARGS.mqtt_host, ARGS.mqtt_port, RETURN_CODE, mqtt.error_string(err)))
        else:
            exit_(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed: - [{}] {}'.format(ARGS.mqtt_host, ARGS.mqtt_port, RETURN_CODE, mqtt.error_string(RETURN_CODE)))
