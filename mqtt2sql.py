#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Script Name: mqtt2sql.py
# Type:        Python Script
# Created:     2017-03-27
# Description: Subscribes to MQTT broker topic and inserts the topic
#              into a sql database table
# Author:      Norbert Richter <norbert.richter@p-r-solution.de>
#
# Parameter:   see mqtt2sql.py --help

import paho.mqtt.client as mqtt
import MySQLdb
import sqlite3
import os, sys
import time, datetime
import signal
import ssl
import argparse

VER = '1.3.0014'

args = {}

def log(msg):
	"""
	Writes a message to stdout and optional logfile

    @param msg: message to output
	"""

	strtime=str(time.strftime("%Y-%m-%d %H:%M:%S"))
	# print strtime+': '+msg
	if args.logfile is not None:
		filename = 	str(time.strftime(args.logfile, time.localtime()))
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

	if args.debug>dbglevel:
		log(msg)

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

	debuglog(1,"on_connect message {}, rc={}".format(message, rc) )
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
	if args.verbose>0:
		log('{} {} [QOS {} Retain {}]'.format(message.topic, message.payload, message.qos, message.retain))

	try:
		debuglog(1,"SQL type is '{}'".format(args.sqltype))
		if args.sqltype=='mysql':
			db = MySQLdb.connect(args.sqlhost, args.sqlusername, args.sqlpassword, args.sqldb)
		elif args.sqltype=='sqlite':
			db = sqlite3.connect(args.sqldb)

		cursor = db.cursor()

		try:
			# INSERT/UPDATE record
			if message.payload!='':
				active = ', active=1'
			else:
				active = ''
			ts = datetime.datetime.fromtimestamp(int(message.timestamp)).strftime('%Y-%m-%d %H:%M:%S')
			if args.sqltype=='mysql':
				cursor.execute("INSERT INTO `{0}` SET `ts`='{1}',`topic`='{2}',`value`='{3}',`qos`='{4}',`retain`='{5}'{6} ON DUPLICATE KEY UPDATE `ts`='{1}',`value`='{3}',`qos`='{4}',`retain`='{5}'{6}".format(args.sqltable, ts, message.topic, message.payload, message.qos, message.retain, active))
			elif args.sqltype=='sqlite':
				# strtime=str(time.strftime("%Y-%m-%d %H:%M:%S"))
				cursor.execute("INSERT OR IGNORE INTO `{0}` (ts,topic,value,qos,retain) VALUES('{1}','{2}','{3}','{4}','{5}')".format(args.sqltable, ts, message.topic, message.payload, message.qos, message.retain))
				cursor.execute("UPDATE `{0}` SET ts='{1}', value='{3}', qos='{4}', retain='{5}' WHERE topic='{2}'".format(args.sqltable, ts, message.topic, message.payload, message.qos, message.retain))

			db.commit()
			debuglog(1,"SQL successful written: table='{}', topic='{}', value='{}', qos='{}', retain='{}'".format(args.sqltable, message.topic, message.payload, message.qos, message.retain))

		except MySQLdb.Error, e:
			try:
				log("MySQL Error [{}]: {}".format(e.args[0], e.args[1]))

			except IndexError:
				log("MySQL Error: {}".format(e))

			# Rollback in case there is any error
			db.rollback()
			log('ERROR adding record to MYSQL')

	except IndexError:
		log("MySQL Error: {}".format(e))

	db.close()


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

def signal_term_handler(signal, frame):
	"""
	Callback called when TERM signal is received

	@param status:
		the signal name
	@param frame:
		current stack frame, could be None or current frame object
	"""

	log('{} v{} end - {}'.format(scriptname, VER, str(signal)))
	sys.exit(0)

def signal_int_handler(signal, frame):
	"""
	Callback called when INT signal is received

	@param status:
		the signal name
	@param frame:
		current stack frame, could be None or current frame object
	"""

	log('{} v{} end - {}'.format(scriptname, VER, str(signal)))
	sys.exit(0)


if __name__ == "__main__":
	# set signal handler
	signal.signal(signal.SIGTERM, signal_term_handler)
	signal.signal(signal.SIGINT, signal_int_handler)

	# Parse command line arguments
	parser = argparse.ArgumentParser(description='MQTT to MySQL transfer.',
									 epilog="Subscribes to MQTT broker topic(s) and copy the values into a given mysql database table")
	mqtt_group = parser.add_argument_group('MQTT Options')
	mqtt_group.add_argument('--mqtthost', '--host', metavar="<mqtthost>", help="MQTT host to connect to (defaults to localhost)", dest='mqtthost', default="localhost")
	mqtt_group.add_argument('--mqttport', '--port', metavar="<mqttport>", help="MQTT network port to connect to (defaults to 1883)", dest='mqttport', default=1883, type=int)
	mqtt_group.add_argument('--mqttusername', '--username', metavar="<mqttusername>", help="MQTT username (defaults to None)", dest='mqttusername', default=None)
	mqtt_group.add_argument('--mqttpassword', '--password', metavar="<mqttpassword>", help="MQTT password (defaults to None)", dest='mqttpassword', default=None)
	mqtt_group.add_argument('--topic', metavar="<topic>", help="MQTT topic to use (defaults to #)", dest='mqtttopic', default='#', nargs='*')
	mqtt_group.add_argument('--cafile', metavar="<cafile>", help="MQTT cafile (defaults to None)", dest='mqttcafile', default=None)
	mqtt_group.add_argument('--certfile', metavar="<certfile>", help="MQTT certfile (defaults to None)", dest='mqttcertfile', default=None)
	mqtt_group.add_argument('--keyfile', metavar="<keyfile>", help="MQTT keyfile (defaults to None)", dest='mqttkeyfile', default=None)
	mqtt_group.add_argument('--insecure', help="suppress TLS verification of MQTT hostname", dest='mqttinsecure', default=False, action='store_true')

	sql_group = parser.add_argument_group('SQL Options')
	sql_group.add_argument('--sqltype', metavar="<sqltype>", help="SQL type to connect (defaults to mysql)", dest='sqltype', default='mysql', choices=['mysql','sqlite'])
	sql_group.add_argument('--sqlhost','--sql_host', metavar="<sqlhost>", help="SQL host to connect (defaults to localhost)", dest='sqlhost', default="localhost")
	sql_group.add_argument('--sqlport','--sql_port', metavar="<sqlport>", help="SQL network port to connect (defaults to 3306)", dest='sqlport', default=3306, type=int)
	sql_group.add_argument('--sqlusername', '--sql_username', metavar="<sqlusername>", help="SQL username (defaults to None)", dest='sqlusername', default=None)
	sql_group.add_argument('--sqlpassword', '--sql_password', metavar="<sqlpassword>", help="SQL password (defaults to None)", dest='sqlpassword', default=None)
	sql_group.add_argument('--sqldb', '--sql_db', metavar="<sqldb>", help="SQL database to use (defaults to None)", dest='sqldb', default=None)
	sql_group.add_argument('--sqltable', '--sql_table', metavar="<sqltable>", help="SQL database table to use (defaults to mqtt)", dest='sqltable', default="mqtt")

	logging_group = parser.add_argument_group('Informational')
	logging_group.add_argument('-l', '--logfile', metavar="<logfile>", help="additional logfile", dest='logfile', default=None)
	logging_group.add_argument('-d', '--debug', help="debug output", dest='debug', action='count')
	logging_group.add_argument('-v', '--verbose', help="verbose output", dest='verbose', action='count')
	logging_group.add_argument('-V', '--version', action='version', version='%(prog)s v'+VER)

	args = parser.parse_args()

	# Get opwn script name
	scriptname = os.path.basename(sys.argv[0])

	# Log program start
	log('{} v{} start'.format(scriptname, VER))

	if args.verbose>0:
		log('  MQTT server: {}:{} {}{}'.format(args.mqtthost, args.mqttport, 'SSL' if (args.mqttcafile is not None) else '', ' (suppress TLS verification)' if args.mqttinsecure else ''))
		log('  MQTT user:   {}'.format(args.mqttusername))
		log('  MQTT topics: {}'.format(args.mqtttopic))
		log('  SQL server:  {}:{}, type \'{}\', db \'{}\', table \'{}\''.format(args.sqlhost, args.sqlport, args.sqltype, args.sqldb, args.sqltable))
		log('  SQL user:    {}'.format(args.sqlusername))
		if args.logfile is not None:
			log('  Log file:    {}'.format(args.logfile))
		if args.debug > 0:
			log('  Debug level: {}'.format(args.debug))

	# Create MQTT client and set callback handler
	mqttc = mqtt.Client(scriptname)
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
	try:
		mqttc.connect(args.mqtthost, args.mqttport, 60)
	except Exception, e:
		exit(3, 'Connection to {}:{} failed: {}'.format(args.mqtthost, args.mqttport, str(e)))

	# Main loop as long as no error occurs
	rc = 0
	while rc == 0:
		try:
			rc = mqttc.loop()
		except Exception, e:
			log('ERROR: loop() - {}'.format(e))
			time.sleep(0.25)

	# disconnect from server
	exit(rc,"MQTT disconnected")
