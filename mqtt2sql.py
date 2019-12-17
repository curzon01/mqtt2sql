#!/usr/bin/env python3
# -*- coding: utf-8 -*-
VER = '2.0.0024'

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
    sudo apt-get install python-pip3 python3-mysqldb
    pip3 install paho-mqtt configargparse


Usage:
    See README.md for more details how to install and use

"""

import os, sys
def ModuleImportError(module):
    er = str(module)
    print("{}. Try 'pip install {}' to install".format(er,er.split(' ')[len(er.split(' '))-1]))
    sys.exit(9)
try:
    import imp
    import paho.mqtt.client as mqtt
    import time, datetime
    import signal
    import logging
    import configargparse
    from threading import Thread, BoundedSemaphore
    from random import random
except ImportError as e:
    ModuleImportError(e)

try:
    imp.find_module('ssl')
    import ssl
    module_ssl = True
except ImportError:
    module_ssl = False
try:
    imp.find_module('MySQLdb')
    import MySQLdb
    module_MySQLdb = True
except ImportError:
    module_MySQLdb = False
try:
    imp.find_module('sqlite3')
    import sqlite3
    module_sqlite3 = True
except ImportError:
    module_sqlite3 = False


args = {}
DEFAULTS = {
     'DEFAULT': 
        { 'configfile'  : None
         ,'logfile'     : None
         ,'debug'       : None
         ,'verbose'     : None
        }
    ,'MQTT':
        { 'host'        : 'localhost'
         ,'port'        : 1883
         ,'username'    : None
         ,'password'    : None
         ,'topic'       : '#'
         ,'cafile'      : None
         ,'certfile'    : None
         ,'keyfile'     : None
         ,'insecure'    : False
         ,'keepalive'   : 60
        }
    ,'SQL':
        { 'type'        : 'mysql' if module_MySQLdb else ('sqlite' if 'module_sqlite3' else None)
         ,'host'        : 'localhost'
         ,'port'        : 3306
         ,'username'    : None
         ,'password'    : None
         ,'db'          : None
         ,'table'       : 'mqtt'
         ,'sqlmaxconnection': 50
        }
}


def log(msg):
    """
    Writes a message to stdout and optional logfile

    @param msg: message to output
    """

    strtime=str(time.strftime("%Y-%m-%d %H:%M:%S"))
    # print strtime+': '+msg
    if args.logfile is not None:
        filename =  str(time.strftime(args.logfile, time.localtime()))
        logfile = open(filename, "a")
        logfile.write(strtime+': '+msg+'\n')
        logfile.close()

def debuglog(dbglevel, msg):
    """
    Writes a message to stdout and optional logfile
    if given dbglevel is >= args.debug

    @param dbglevel:
        debug level used
        e.g.
        if -d is given one time and dbglevel is 1, then msg is output
        if -d is given one time and dbglevel is 2, then msg will not output
        if -d is given two times and dbglevel is 2, then msg will output
    @param msg: message to output
    """

    if args.debug is not None and args.debug>dbglevel:
        log(msg)

def write2sql(message):
    """
    Called as thread from mqtt backlog handler when a message has been
    received on a topic that the client subscribes to.

    @param message:
        an instance of MQTTMessage.
        This is a class with members topic, payload, qos, retain.
    """
    db = None
    
    debuglog(1,"SQL type is '{}'".format(args.sqltype))
    try:
        if args.sqltype=='mysql':
            db = MySQLdb.connect(args.sqlhost, args.sqlusername, args.sqlpassword, args.sqldb)
        elif args.sqltype=='sqlite':
            db = sqlite3.connect(args.sqldb)
    except IndexError:
        log("MySQL Error: {}".format(e))

    transactionretry = 10
    while transactionretry>0:
        cursor = db.cursor()
        try:
            # INSERT/UPDATE record
            if message.payload!='':
                active = ', active=1'
            else:
                active = ''
            ts = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')
            payload = message.payload
            if not isinstance(payload, str):
                payload = str(payload, 'utf-8',errors='ignore')
            if args.sqltype=='mysql':
                sql="INSERT INTO `{0}` SET `ts`='{1}',`topic`='{2}',`value`='{3}',`qos`='{4}',`retain`='{5}'{6} ON DUPLICATE KEY UPDATE `ts`='{1}',`value`='{3}',`qos`='{4}',`retain`='{5}'{6}".format(args.sqltable, ts, message.topic, payload, message.qos, message.retain, active)
                debuglog(4,"SQL exec: '{}'".format(sql))
                cursor.execute(sql)
            elif args.sqltype=='sqlite':
                # strtime=str(time.strftime("%Y-%m-%d %H:%M:%S"))
                sql1="INSERT OR IGNORE INTO `{0}` (ts,topic,value,qos,retain) VALUES('{1}','{2}','{3}','{4}','{5}')".format(args.sqltable, ts, message.topic, payload, message.qos, message.retain)
                sql2="UPDATE `{0}` SET ts='{1}', value='{3}', qos='{4}', retain='{5}' WHERE topic='{2}'".format(args.sqltable, ts, message.topic, payload, message.qos, message.retain)
                sql=sql1 + '\n' + sql2
                debuglog(4,"SQL exec: '{}'".format(sql))
                cursor.execute(sql1)
                cursor.execute(sql2)

            db.commit()
            debuglog(1,"SQL successful written: table='{}', topic='{}', value='{}', qos='{}', retain='{}'".format(args.sqltable, message.topic, payload, message.qos, message.retain))
            transactionretry = 0

        except MySQLdb.Error as e:
            retry = args.sqltype=='mysql' and e.args[0] in [1040,1205,1213]

            if retry:
                transactionretry -= 1
                transactiondelay = random()
                if args.sqltype=='mysql' and e.args[0] in [1040]:
                    transactiondelay *= 2
                time.sleep(transactiondelay)
            else:
                log("SQL Error [{}]: {}".format(e.args[0], e.args[1]))
                log("SQL Exec: {}".format(sql))
                # Rollback in case there is any error
                db.rollback()
                transactionretry = 0

        finally:
            cursor.close()

    db.close()
    pool_sqlconnections.release()
    

def on_connect(client, userdata, message, rc):
    """
    Called when the broker responds to our connection request.

    @param client:
        the client instance for this callback
    @param userdata:
        the private user data as set in Client() or userdata_set()
    @param message:
        response message sent by the broker
    @param rc:
        the connection result
    """

    if rc==0:
        errtext="Connection successful"
    elif rc==1:
        errtext="Connection refused: Unacceptable protocol version"
    elif rc==2:
        errtext="Connection refused: Identifier rejected"
    elif rc==3:
        errtext="Connection refused: Server unavailable"
    elif rc==4:
        errtext="Connection refused: Bad user name or password"
    elif rc==5:
        errtext="Connection refused: Not authorized"
    elif rc>5:
        errtext="Connection refused: Unknown reason"
    debuglog(1,"MQTT on_connect() returns rc={}: {}".format(rc, errtext) )
    for topic in args.mqtttopic:
        debuglog(1,"subscribing to topic {}".format(topic))
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
    if args.verbose is not None and args.verbose>0:
        log('{} {} [QOS {} Retain {}]'.format(message.topic, message.payload, message.qos, message.retain))

    pool_sqlconnections.acquire()
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
    debuglog(2,"on_publish() mid {}".format(mid))

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
    debuglog(1,"on_subscribe() mid {}, granted_qos {}".format(str(mid), str(granted_qos)) )

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

    debuglog(2,"on_log() {}".format(string))

def exit(status=0, message="end"):
    """
    Called when the program should be exit

    @param status:
        the exit status program returns to callert
    @param message:
        the message logged before exit
    """

    log(message)
    sys.exit(status)


class SignalHandler:
    """
    Signal Handler Class
    """
    def __init__(self):
        signal.signal(signal.SIGINT,  self.exitus)
        signal.signal(signal.SIGTERM, self.exitus)

    def exitus(self, signal, frame):
        log('{} v{} end - signal {}'.format(scriptname, VER, signal))
        sys.exit(signal)


if __name__ == "__main__":
    # set signal handler
    sig = SignalHandler()

    # Parse command line arguments
    parser = configargparse.ArgumentParser(description='MQTT to MySQL bridge. Subscribes to MQTT broker topic(s) and write values into a database.',
                                     epilog="There are no required arguments, defaults are displayed using --help.")
    parser.add_argument('-c', '--configfile',               metavar="<filename>",       dest='configfile',is_config_file=True,default=DEFAULTS['DEFAULT']['configfile'],    help="Config file, can be used instead of command parameter (default {})".format(DEFAULTS['DEFAULT']['configfile']))

    mqtt_group = parser.add_argument_group('MQTT Options')
    mqtt_group.add_argument('--mqtthost','--host',          metavar='<host>',           dest='mqtthost',                    default=DEFAULTS['MQTT']['host'],           help="host to connect to (default '{}')".format(DEFAULTS['MQTT']['host']))
    mqtt_group.add_argument('--mqttport','--port',          metavar='<port>',           dest='mqttport',    type=int,       default=DEFAULTS['MQTT']['port'],           help="port to connect to (default {})".format(DEFAULTS['MQTT']['port']))
    mqtt_group.add_argument('--mqttusername','--username',  metavar='<username>',       dest='mqttusername',                default=DEFAULTS['MQTT']['username'],       help="username (default {})".format(DEFAULTS['MQTT']['username']))
    mqtt_group.add_argument('--mqttpassword','--password',  metavar='<password>',       dest='mqttpassword',                default=DEFAULTS['MQTT']['password'],       help="password (default {})".format(DEFAULTS['MQTT']['password']))
    mqtt_group.add_argument('--topic',                      metavar='<topic>',          dest='mqtttopic',   nargs='*',      default=DEFAULTS['MQTT']['topic'],          help="topic to use (default {})".format(DEFAULTS['MQTT']['topic']))
    mqtt_group.add_argument('--cafile',                     metavar='<cafile>',         dest='mqttcafile',                  default=DEFAULTS['MQTT']['cafile'],         help="cafile (default {})".format(DEFAULTS['MQTT']['cafile']) if module_ssl else configargparse.SUPPRESS)
    mqtt_group.add_argument('--certfile',                   metavar='<certfile>',       dest='mqttcertfile',                default=DEFAULTS['MQTT']['certfile'],       help="certfile (default {})".format(DEFAULTS['MQTT']['certfile']) if module_ssl else configargparse.SUPPRESS)
    mqtt_group.add_argument('--keyfile',                    metavar='<keyfile>',        dest='mqttkeyfile',                 default=DEFAULTS['MQTT']['keyfile'],        help="keyfile (default {})".format(DEFAULTS['MQTT']['keyfile']) if module_ssl else configargparse.SUPPRESS)
    mqtt_group.add_argument('--insecure',                                               dest='mqttinsecure',action='store_true',default=DEFAULTS['MQTT']['insecure'],   help="suppress TLS verification (default {})".format(DEFAULTS['MQTT']['insecure']) if module_ssl else configargparse.SUPPRESS)
    mqtt_group.add_argument('--keepalive',                  metavar='<sec>',            dest='keepalive',   type=int,       default=DEFAULTS['MQTT']['keepalive'],      help="keepalive timeout for the client (default {})".format(DEFAULTS['MQTT']['keepalive']))
    
    sql_group = parser.add_argument_group('SQL Options')
    sqltypes = []
    if module_MySQLdb:
        sqltypes.append('mysql')
    if module_sqlite3:
        sqltypes.append('sqlite')
    if len(sqltypes)==0:
        exit(3, 'Either module MySQLdb or sqlite must be installed')

    sql_group.add_argument('--sqltype',                     metavar='<type>',           dest='sqltype',     choices=sqltypes,default=DEFAULTS['SQL']['type'],           help="server type {} (default '{}')".format(sqltypes, DEFAULTS['SQL']['type']))
    sql_group.add_argument('--sqlhost',                     metavar='<host>',           dest='sqlhost',                     default=DEFAULTS['SQL']['host'],            help="host to connect (default '{}')".format(DEFAULTS['SQL']['host']))
    sql_group.add_argument('--sqlport',                     metavar='<port>',           dest='sqlport',     type=int,       default=DEFAULTS['SQL']['port'],            help="port to connect (default {})".format(DEFAULTS['SQL']['port']))
    sql_group.add_argument('--sqlusername',                 metavar='<username>',       dest='sqlusername',                 default=DEFAULTS['SQL']['username'],        help="username (default {})".format(DEFAULTS['SQL']['username']))
    sql_group.add_argument('--sqlpassword',                 metavar='<password>',       dest='sqlpassword',                 default=DEFAULTS['SQL']['password'],        help="password (default {})".format(DEFAULTS['SQL']['password']))
    sql_group.add_argument('--sqldb',                       metavar='<db>',             dest='sqldb',                       default=DEFAULTS['SQL']['db'],              help="database to use (default '{}')".format(DEFAULTS['SQL']['db']))
    sql_group.add_argument('--sqltable',                    metavar='<table>',          dest='sqltable',                    default=DEFAULTS['SQL']['table'],           help="table to use (default '{}')".format(DEFAULTS['SQL']['table']))
    sql_group.add_argument('--sqlmaxconnection',            metavar='<num>',            dest='sqlmaxconnection',type=int,   default=DEFAULTS['SQL']['sqlmaxconnection'],help="maximum number of parallel connections (default {})".format(DEFAULTS['SQL']['sqlmaxconnection']))

    logging_group = parser.add_argument_group('Informational')
    logging_group.add_argument('-l','--logfile',            metavar='<filename>',       dest='logfile',                     default=DEFAULTS['DEFAULT']['logfile'],     help="optional logfile (default {}".format(DEFAULTS['DEFAULT']['logfile']))
    logging_group.add_argument('-d','--debug',                                          dest='debug', action='count',                                                   help='debug output')
    logging_group.add_argument('-v','--verbose',                                        dest='verbose', action='count',                                                 help='verbose output')
    logging_group.add_argument('-V','--version',                                        action='version',version='%(prog)s v'+VER)

    args = parser.parse_args()

    # Get opwn script name
    scriptname = os.path.basename(sys.argv[0])

    # Log program start
    log('{} v{} start'.format(scriptname, VER))

    if args.verbose is not None and args.verbose>0:
        log('  MQTT server: {}:{} {}{} keepalive {}'.format(args.mqtthost, args.mqttport, 'SSL' if (args.mqttcafile is not None) else '', ' (suppress TLS verification)' if args.mqttinsecure else '', args.keepalive))
        log('  MQTT user:   {}'.format(args.mqttusername))
        log('  MQTT topics: {}'.format(args.mqtttopic))
        log('  SQL server:  {}:{}, type \'{}\', db \'{}\', table \'{}\' ({})'.format(args.sqlhost, args.sqlport, args.sqltype, args.sqldb, args.sqltable, args.sqlmaxconnection))
        log('  SQL user:    {}'.format(args.sqlusername))
        if args.logfile is not None:
            log('  Log file:    {}'.format(args.logfile))
        if args.debug is not None and args.debug > 0:
            log('  Debug level: {}'.format(args.debug))
        if args.verbose is not None and args.verbose > 0:
            log('  Verbose level: {}'.format(args.verbose))

    pool_sqlconnections = BoundedSemaphore(value=args.sqlmaxconnection)

    # Create MQTT client and set callback handler
    userdata = {
        'haveresponse' : False,
        'starttime'    : time.time(),
    }
    mqttc = mqtt.Client('{}-{:d}'.format(scriptname, os.getpid()), clean_session=True, userdata=userdata)
    if args.debug is not None and args.debug>0:
        if args.debug==1:
            logging.basicConfig(level=logging.DEBUG)
        elif args.debug==2:
            logging.basicConfig(level=logging.WARNING)
        elif args.debug==3:
            logging.basicConfig(level=logging.INFO)
        elif args.debug>=4:
            logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)
        mqttc.enable_logger(logger)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    mqttc.on_log = on_log

    # cafile controls TLS usage
    if args.mqttcafile is not None:
        if args.mqttcertfile is not None:
            mqttc.tls_set(args.mqttcafile,
            certfile=args.mqttcertfile,
            keyfile=args.mqttkeyfile,
            cert_reqs=ssl.CERT_REQUIRED)
        else:
            mqttc.tls_set(args.mqttcafile, cert_reqs=ssl.CERT_REQUIRED)
        mqttc.tls_insecure_set(args.mqttinsecure)

    # username & password may be None
    if args.mqttusername is not None:
        mqttc.username_pw_set(args.mqttusername, args.mqttpassword)

    # Attempt to connect to broker. If this fails, issue CRITICAL
    debuglog(1,"mqttc.connect({}, {}, 60)".format(args.mqtthost, args.mqttport))
    try:
        rc=mqttc.connect(args.mqtthost, args.mqttport, args.keepalive)
        debuglog(1,"mqttc.connect() returns {}".format(rc))
    except Exception as e:
        exit(3, 'Connection to {}:{} failed: {}'.format(args.mqtthost, args.mqttport, str(e)))

    while True:
        # Main loop as long as no error occurs
        rc = 0
        while userdata['haveresponse'] == False and rc == 0:
            try:
                rc = mqttc.loop()
            except Exception as e:
                log('ERROR: loop() - {}'.format(e))
                time.sleep(0.25)

        # disconnect from server
        log('MQTT disconnected')
        try:
            rc = mqttc.reconnect()
            debuglog(1,"mqttc.reconnect() returns {}".format(rc))
            log('MQTT reconnect')
        except Exception as e:
            exit(3, 'Connection to {}:{} failed: {}'.format(args.mqtthost, args.mqttport, str(e)))
