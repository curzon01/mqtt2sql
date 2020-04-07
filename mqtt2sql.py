#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
VER = '2.0.0028'

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
    print("{}. Try 'pip install {}' to install".format(err_str, err_str.split(' ')[len(err_str.split(' '))-1]))
    sys.exit(9)

# pylint: disable=wrong-import-position
import os
import sys
try:
    import imp
    import paho.mqtt.client as mqtt
    import time
    import datetime
    import signal
    import logging
    import configargparse
    from threading import Thread, BoundedSemaphore
    from random import random
except ImportError as err:
    module_import_error(err)

try:
    imp.find_module('ssl')
    import ssl
    MODULE_SSL = True
except ImportError:
    MODULE_SSL = False
try:
    imp.find_module('MySQLdb')
    import MySQLdb
    MODULE_MYSQLDB = True
except ImportError:
    MODULE_MYSQLDB = False
try:
    imp.find_module('sqlite3')
    import sqlite3
    MODULE_SQLITE3 = True
except ImportError:
    MODULE_SQLITE3 = False
# pylint: enable=wrong-import-position

EXIT_CODE = ExitCode.OK
SQLTYPES = {'mysql':'MySQl', 'sqlite':'SQLite'} 
ARGS = {}
DEFAULTS = {
    'DEFAULT': {
        'configfile': None,
        'logfile'   : None,
        'debug'     : None,
        'verbose'   : None
    },
    'MQTT':{
        'host'      : 'localhost',
        'port'      : 1883,
        'username'  : None,
        'password'  : None,
        'topic'     : '#',
        'cafile'    : None,
        'certfile'  : None,
        'keyfile'   : None,
        'insecure'  : False,
        'keepalive' : 60
    },
    'SQL':{
        'type'      : 'mysql' if MODULE_MYSQLDB else ('sqlite' if MODULE_SQLITE3 else None),
        'host'      : 'localhost',
        'port'      : 3306,
        'username'  : None,
        'password'  : None,
        'db'        : None,
        'table'     : 'mqtt',
        'sqlmaxconnection': 50
    }
}


def log(msg):
    """
    Writes a message to stdout and optional logfile

    @param msg: message to output
    """

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
        nonlocal db_connection, sql, transactionretry

        typestr = SQLTYPES[ARGS.sqltype]

        debuglog(1, "{} ERROR: {}".format(typestr, error_str))
        if retry_condition:
            transactionretry -= 1
            transactiondelay = random()
            transactiondelay *= 2
            time.sleep(transactiondelay)
        else:
            log("{} ERROR {}".format(typestr, error_str))
            log("{} statement: {}".format(typestr, sql))
            # Rollback in case there is any error
            db_connection.rollback()
            transactionretry = 0

    db_connection = None

    debuglog(1, "SQL type is '{}'".format(SQLTYPES[ARGS.sqltype]))
    try:
        if ARGS.sqltype == 'mysql':
            if ARGS.sqlusername is not None and ARGS.sqlpassword is not None:
                db_connection = MySQLdb.connect(ARGS.sqlhost, ARGS.sqlusername, ARGS.sqlpassword, ARGS.sqldb)
            else:
                db_connection = MySQLdb.connect(ARGS.sqlhost)
        elif ARGS.sqltype == 'sqlite':
            db_connection = sqlite3.connect(ARGS.sqldb)
    except Exception as err:    # pylint: disable=broad-except
        exit_(ExitCode.SQL_CONNECTION_ERROR, "SQL conncetion error: {}".format(err))

    transactionretry = 10
    while transactionretry > 0:
        cursor = db_connection.cursor()
        try:
            # INSERT/UPDATE record
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload = message.payload
            if not isinstance(payload, str):
                payload = str(payload, 'utf-8', errors='ignore')
            if ARGS.sqltype == 'mysql':
                sql = "INSERT INTO `{0}` \
                       SET `ts`='{1}',`topic`='{2}',`value`='{3}',`qos`='{4}',`retain`='{5}' \
                       ON DUPLICATE KEY UPDATE `ts`='{1}',`value`='{3}',`qos`='{4}',`retain`='{5}'"\
                    .format(
                        ARGS.sqltable,
                        timestamp,
                        message.topic,
                        payload,
                        message.qos,
                        message.retain
                    )
                debuglog(4, "SQL exec: '{}'".format(sql))
                cursor.execute(sql)
            elif ARGS.sqltype == 'sqlite':
                # strtime=str(time.strftime("%Y-%m-%d %H:%M:%S"))
                sql1 = "INSERT OR IGNORE INTO `{0}` \
                        (ts,topic,value,qos,retain) \
                        VALUES('{1}','{2}','{3}','{4}','{5}')"\
                    .format(
                        ARGS.sqltable,
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
                        ARGS.sqltable,
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
            debuglog(1, "SQL successful written: table='{}', topic='{}', value='{}', qos='{}', retain='{}'".format(ARGS.sqltable, message.topic, payload, message.qos, message.retain))
            transactionretry = 0

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

    if return_code == mqtt.CONNACK_ACCEPTED:
        errtext = "Connection successful"
    elif return_code == mqtt.CONNACK_REFUSED_PROTOCOL_VERSION:
        errtext = "Connection refused: Unacceptable protocol version"
    elif return_code == mqtt.CONNACK_REFUSED_IDENTIFIER_REJECTED:
        errtext = "Connection refused: Identifier rejected"
    elif return_code == mqtt.CONNACK_REFUSED_SERVER_UNAVAILABLE:
        errtext = "Connection refused: Server unavailable"
    elif return_code == mqtt.CONNACK_REFUSED_BAD_USERNAME_PASSWORD:
        errtext = "Connection refused: Bad user name or password"
    elif return_code == mqtt.CONNACK_REFUSED_NOT_AUTHORIZED:
        errtext = "Connection refused: Not authorized"
    elif return_code > mqtt.CONNACK_REFUSED_NOT_AUTHORIZED:
        errtext = "Connection refused: Unknown reason"
    debuglog(2, "MQTT on_connect({},{},{},{}): {}".format(client, userdata, message, return_code, errtext))
    for topic in ARGS.mqtttopic:
        debuglog(1, "subscribing to topic {}".format(topic))
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
        debuglog(2, "SignalHandler.exitus({},{})".format(signal_, frame_))
        exit_(signal, '{} v{} end - signal {}'.format(SCRIPTNAME, VER, signal_))


if __name__ == "__main__":
    # set signal handler
    SIG = SignalHandler()

    # Parse command line arguments
    PARSER = configargparse.ArgumentParser(\
            description='MQTT to MySQL bridge. Subscribes to MQTT broker topic(s) and write values into a database.',
            epilog="There are no required arguments, defaults are displayed using --help."
            )
    PARSER.add_argument(
        '-c', '--configfile',
        metavar="<filename>",
        dest='configfile',
        is_config_file=True,
        default=DEFAULTS['DEFAULT']['configfile'],
        help="Config file, can be used instead of command parameter (default {})".format(DEFAULTS['DEFAULT']['configfile'])
    )

    MQTT_GROUP = PARSER.add_argument_group('MQTT Options')
    MQTT_GROUP.add_argument(
        '--mqtthost', '--host',
        metavar='<host>',
        dest='mqtthost',
        default=DEFAULTS['MQTT']['host'],
        help="host to connect to (default '{}')".format(DEFAULTS['MQTT']['host'])
    )
    MQTT_GROUP.add_argument(
        '--mqttport', '--port',
        metavar='<port>',
        dest='mqttport',
        type=int,
        default=DEFAULTS['MQTT']['port'],
        help="port to connect to (default {})".format(DEFAULTS['MQTT']['port'])
    )
    MQTT_GROUP.add_argument(
        '--mqttusername', '--username',
        metavar='<username>',
        dest='mqttusername',
        default=DEFAULTS['MQTT']['username'],
        help="username (default {})".format(DEFAULTS['MQTT']['username'])
    )
    MQTT_GROUP.add_argument(
        '--mqttpassword', '--password',
        metavar='<password>',
        dest='mqttpassword',
        default=DEFAULTS['MQTT']['password'],
        help="password (default {})".format(DEFAULTS['MQTT']['password']))
    MQTT_GROUP.add_argument(
        '--topic',
        metavar='<topic>',
        dest='mqtttopic',
        nargs='*',
        default=DEFAULTS['MQTT']['topic'],
        help="topic to use (default {})".format(DEFAULTS['MQTT']['topic']))
    MQTT_GROUP.add_argument(
        '--cafile',
        metavar='<cafile>',
        dest='mqttcafile',
        default=DEFAULTS['MQTT']['cafile'],
        help="cafile (default {})".format(DEFAULTS['MQTT']['cafile']) if MODULE_SSL else configargparse.SUPPRESS)
    MQTT_GROUP.add_argument(
        '--certfile',
        metavar='<certfile>',
        dest='mqttcertfile',
        default=DEFAULTS['MQTT']['certfile'],
        help="certfile (default {})".format(DEFAULTS['MQTT']['certfile']) if MODULE_SSL else configargparse.SUPPRESS)
    MQTT_GROUP.add_argument(
        '--keyfile',
        metavar='<keyfile>',
        dest='mqttkeyfile',
        default=DEFAULTS['MQTT']['keyfile'],
        help="keyfile (default {})".format(DEFAULTS['MQTT']['keyfile']) if MODULE_SSL else configargparse.SUPPRESS)
    MQTT_GROUP.add_argument(
        '--insecure',
        dest='mqttinsecure',
        action='store_true',
        default=DEFAULTS['MQTT']['insecure'],
        help="suppress TLS verification (default {})".format(DEFAULTS['MQTT']['insecure']) if MODULE_SSL else configargparse.SUPPRESS)
    MQTT_GROUP.add_argument(
        '--keepalive',
        metavar='<sec>',
        dest='keepalive',
        type=int,
        default=DEFAULTS['MQTT']['keepalive'],
        help="keepalive timeout for the client (default {})".format(DEFAULTS['MQTT']['keepalive']))

    SQL_GROUP = PARSER.add_argument_group('SQL Options')
    SQL_CHOICES = []
    if MODULE_MYSQLDB:
        SQL_CHOICES.append('mysql')
    if MODULE_SQLITE3:
        SQL_CHOICES.append('sqlite')
    if len(SQL_CHOICES) == 0:
        exit_(ExitCode.MISSING_MODULE, 'Either module MySQLdb or sqlite must be installed')

    SQL_GROUP.add_argument(
        '--sqltype',
        metavar='<type>',
        dest='sqltype',
        choices=SQL_CHOICES,
        default=DEFAULTS['SQL']['type'],
        help="server type {} (default '{}')".format(SQL_CHOICES, DEFAULTS['SQL']['type']))
    SQL_GROUP.add_argument(
        '--sqlhost',
        metavar='<host>',
        dest='sqlhost',
        default=DEFAULTS['SQL']['host'],
        help="host to connect (default '{}')".format(DEFAULTS['SQL']['host']))
    SQL_GROUP.add_argument(
        '--sqlport',
        metavar='<port>',
        dest='sqlport',
        type=int,
        default=DEFAULTS['SQL']['port'],
        help="port to connect (default {})".format(DEFAULTS['SQL']['port']))
    SQL_GROUP.add_argument(
        '--sqlusername',
        metavar='<username>',
        dest='sqlusername',
        default=DEFAULTS['SQL']['username'],
        help="username (default {})".format(DEFAULTS['SQL']['username']))
    SQL_GROUP.add_argument(
        '--sqlpassword',
        metavar='<password>',
        dest='sqlpassword',
        default=DEFAULTS['SQL']['password'],
        help="password (default {})".format(DEFAULTS['SQL']['password']))
    SQL_GROUP.add_argument(
        '--sqldb',
        metavar='<db>',
        dest='sqldb',
        default=DEFAULTS['SQL']['db'],
        help="database to use (default '{}')".format(DEFAULTS['SQL']['db']))
    SQL_GROUP.add_argument(
        '--sqltable',
        metavar='<table>',
        dest='sqltable',
        default=DEFAULTS['SQL']['table'],
        help="table to use (default '{}')".format(DEFAULTS['SQL']['table']))
    SQL_GROUP.add_argument(
        '--sqlmaxconnection',
        metavar='<num>',
        dest='sqlmaxconnection',
        type=int,
        default=DEFAULTS['SQL']['sqlmaxconnection'],
        help="maximum number of simultaneous connections (default {})".format(DEFAULTS['SQL']['sqlmaxconnection']))

    LOGGING_GROUP = PARSER.add_argument_group('Informational')
    LOGGING_GROUP.add_argument(
        '-l', '--logfile',
        metavar='<filename>',
        dest='logfile',
        default=DEFAULTS['DEFAULT']['logfile'],
        help="optional logfile (default {}".format(DEFAULTS['DEFAULT']['logfile']))
    LOGGING_GROUP.add_argument(
        '-d', '--debug',
        dest='debug',
        action='count',
        help='debug output')
    LOGGING_GROUP.add_argument(
        '-v', '--verbose',
        dest='verbose',
        action='count',
        help='verbose output')
    LOGGING_GROUP.add_argument(
        '-V', '--version',
        action='version',
        version='%(prog)s v'+VER)

    ARGS = PARSER.parse_args()

    # Get own script name
    SCRIPTNAME = os.path.basename(sys.argv[0])

    # Log program start
    log('{} v{} start'.format(SCRIPTNAME, VER))

    if ARGS.verbose is not None and ARGS.verbose > 0:
        log('  MQTT server: {}:{} {}{} keepalive {}'.format(ARGS.mqtthost, ARGS.mqttport, 'SSL' if (ARGS.mqttcafile is not None) else '', ' (suppress TLS verification)' if ARGS.mqttinsecure else '', ARGS.keepalive))
        log('       user:   {}'.format(ARGS.mqttusername))
        log('       topics: {}'.format(ARGS.mqtttopic))
        log('  SQL  type:   {}'.format(SQLTYPES[ARGS.sqltype]))
        log('       server: {}:{} [max {} connections]'.format(ARGS.sqlhost, ARGS.sqlport, ARGS.sqlmaxconnection))
        log('       db:     {}'.format(ARGS.sqldb))
        log('       table:  {}'.format(ARGS.sqltable))
        log('       user:   {}'.format(ARGS.sqlusername))
        if ARGS.logfile is not None:
            log('  Log file:    {}'.format(ARGS.logfile))
        if ARGS.debug is not None and ARGS.debug > 0:
            log('  Debug level: {}'.format(ARGS.debug))
        if ARGS.verbose is not None and ARGS.verbose > 0:
            log('  Verbose level: {}'.format(ARGS.verbose))

    POOL_SQLCONNECTIONS = BoundedSemaphore(value=ARGS.sqlmaxconnection)

    # Create MQTT client and set callback handler
    USERDATA = {
        'haveresponse' : False,
        'starttime'    : time.time(),
    }
    MQTTC = mqtt.Client('{}-{:d}'.format(SCRIPTNAME, os.getpid()), clean_session=True, userdata=USERDATA)
    if ARGS.debug is not None and ARGS.debug > 0:
        if ARGS.debug == 1:
            logging.basicConfig(level=logging.DEBUG)
        elif ARGS.debug == 2:
            logging.basicConfig(level=logging.WARNING)
        elif ARGS.debug == 3:
            logging.basicConfig(level=logging.INFO)
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
    if ARGS.mqttcafile is not None:
        if ARGS.mqttcertfile is not None:
            MQTTC.tls_set(
                ca_certs=ARGS.mqttcafile,
                certfile=ARGS.mqttcertfile,
                keyfile=ARGS.mqttkeyfile,
                cert_reqs=ssl.CERT_REQUIRED
            )
        else:
            MQTTC.tls_set(ARGS.mqttcafile, cert_reqs=ssl.CERT_REQUIRED)
        MQTTC.tls_insecure_set(ARGS.mqttinsecure)

    # username & password may be None
    if ARGS.mqttusername is not None:
        MQTTC.username_pw_set(ARGS.mqttusername, ARGS.mqttpassword)

    # Attempt to connect to broker. If this fails, issue CRITICAL
    debuglog(1, "MQTTC.connect({}, {}, {})".format(ARGS.mqtthost, ARGS.mqttport, ARGS.keepalive))
    try:
        RETURN_CODE = MQTTC.connect(ARGS.mqtthost, ARGS.mqttport, ARGS.keepalive)
        debuglog(1, "MQTTC.connect() returns {}".format(RETURN_CODE))
    except Exception as err:    # pylint: disable=broad-except
        exit_(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed - [{}] {} - "{}"'.format(ARGS.mqtthost, ARGS.mqttport, RETURN_CODE, mqtt.error_string(RETURN_CODE), err))

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
                exit_(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed - [{}] {}'.format(ARGS.mqtthost, ARGS.mqttport, RETURN_CODE, mqtt.error_string(err)))
        else:
            exit_(ExitCode.MQTT_CONNECTION_ERROR, '{}:{} failed: - [{}] {}'.format(ARGS.mqtthost, ARGS.mqttport, RETURN_CODE, mqtt.error_string(RETURN_CODE)))
