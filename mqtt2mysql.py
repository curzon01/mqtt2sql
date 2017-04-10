#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Script Name: mqtt2mysql.py
# Type:        Python Script
# Created:     2017-03-27
# Description: Subscribes to MQTT broker topic and inserts the topic
#              into a mysql database table
# Author:      Norbert Richter <norbert.richter@p-r-solution.de>
#
# Parameter:   

import paho.mqtt.client as mqtt
import MySQLdb
import os, sys
import time
import signal
import argparse

VERSION = '1.1.0006'

args = {}

def log(msg):
	if args.verbose>0:
		print msg
	if args.logfile is not None:
		logfile = open(args.logfile, "a")
		logfile.write(str(time.strftime("%Y-%m-%d %H:%M:%S")) + ': ' + msg + '\n')
		logfile.close()

def on_connect(client, userdata, message):
	"""
	Upon successfully being connected, we subscribe to the check_topic
	"""

	if args.debug>0:
		log("on_connect message %s" % str(message))
	for topic in args.mqtt_topic:
		if args.debug>0:
			log("subscribing to topic %s" % topic)
		client.subscribe(topic, 0)


def on_message(client, userdata, message):
	log(message.topic+' '+str(message.payload) + ' [QOS '+str(message.qos)+']')

	try:
		db = MySQLdb.connect(args.sql_host, args.sql_username, args.sql_password, args.sql_db)
		cursor = db.cursor()

		try:
			# INSERT or UPDATE
			cursor.execute("INSERT INTO `%s` SET `topic`='%s', `value`='%s' ON DUPLICATE KEY UPDATE `value`='%s'" % (args.sql_table, message.topic, str(message.payload), str(message.payload)))
			db.commit()
			if args.debug>0:
				log("SQL successful written: table='%s', topic='%s', value'%s'" % (args.sql_table, message.topic, str(message.payload)))

		except MySQLdb.Error, e:
			try:
				log("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))

			except IndexError:
				log("MySQL Error: %s" % str(e))

			# Rollback in case there is any error
			db.rollback()
			log('ERROR adding record to MYSQL')

	except IndexError:
		log("MySQL Error: %s" % str(e))

	db.close()


def on_publish(client, userdata, mid):
	if args.debug>0:
		log("on_publish() mid %s" % str(mid))

def on_subscribe(client, userdata, mid, granted_qos):
	if args.debug>0:
		log("on_subscribe() mid %s, granted_qos %s" % (str(mid), str(granted_qos)) )

def on_log(client, userdata, level, string):
	if args.debug>0:
		log("on_log() %s" % string)

def exit(status=0, message="end"):
	log("%s" % message)
	sys.exit(status)

def signal_term_handler(signal, frame):
	log('%s v%s end - %s' % (scriptname, VERSION, str(signal)))
	sys.exit(0)


# set signal handler
signal.signal(signal.SIGTERM, signal_term_handler)

parser = argparse.ArgumentParser()
mqtt_group = parser.add_argument_group('MQTT Options')
mqtt_group.add_argument('--host', metavar="<mqtt_host>", help="MQTT host to connect to (defaults to localhost)", dest='mqtt_host', default="localhost")
mqtt_group.add_argument('--port', metavar="<mqtt_port>", help="MQTT network port to connect to (defaults to 1883)", dest='mqtt_port', default=1883, type=int)
mqtt_group.add_argument('--username', metavar="<mqtt_username>", help="MQTT username (defaults to None)", dest='mqtt_username', default=None)
mqtt_group.add_argument('--password', metavar="<mqtt_password>", help="MQTT password (defaults to None)", dest='mqtt_password', default=None)
mqtt_group.add_argument('--topic', metavar="<topic>", help="MQTT topic to use (defaults to #)", dest='mqtt_topic', default='#', nargs='*')
mqtt_group.add_argument('--cafile', metavar="<cafile>", help="MQTT cafile (defaults to None)", dest='mqtt_cafile', default=None)
mqtt_group.add_argument('--certfile', metavar="<certfile>", help="MQTT certfile (defaults to None)", dest='mqtt_certfile', default=None)
mqtt_group.add_argument('--keyfile', metavar="<keyfile>", help="MQTT keyfile (defaults to None)", dest='mqtt_keyfile', default=None)
mqtt_group.add_argument('--insecure', help="suppress TLS verification of MQTT hostname", dest='mqtt_insecure', default=False, action='store_true')

sql_group = parser.add_argument_group('SQL Options')
sql_group.add_argument('--sql_host', metavar="<sql_host>", help="SQL host to connect to (defaults to localhost)", dest='sql_host', default="localhost")
sql_group.add_argument('--sql_port', metavar="<sql_port>", help="SQL network port to connect to (defaults to 3306)", dest='sql_port', default=3306, type=int)
sql_group.add_argument('--sql_username', metavar="<sql_username>", help="SQL username (defaults to None)", dest='sql_username', default=None)
sql_group.add_argument('--sql_password', metavar="<sql_password>", help="SQL password (defaults to None)", dest='sql_password', default=None)
sql_group.add_argument('--sql_db', metavar="<sql_db>", help="SQL database to use (defaults to None)", dest='sql_db', default=None)
sql_group.add_argument('--sql_table', metavar="<sql_table>", help="SQL database table to use (defaults to mqtt)", dest='sql_table', default="mqtt")

logging_group = parser.add_argument_group('Logging')
logging_group.add_argument('-l', '--logfile', metavar="<logfile>", help="additional logfile", dest='logfile', default=None)
logging_group.add_argument('-v', '--verbose', help="verbose output", dest='verbose', action='count')

debug_group = parser.add_argument_group('Debug Options')
debug_group.add_argument('-d', '--debug', help="debug output", dest='debug', action='count')

args = parser.parse_args()

scriptname = os.path.basename(sys.argv[0])

log('%s v%s start' % (scriptname, VERSION))

mqttc = mqtt.Client(scriptname)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
mqttc.on_log = on_log

# cafile controls TLS usage
if args.mqtt_cafile is not None:
	if args.mqtt_certfile is not None:
		mqttc.tls_set(args.mqtt_cafile,
		certfile=args.mqtt_certfile,
		keyfile=args.mqtt_keyfile,
		cert_reqs=ssl.CERT_REQUIRED)
	else:
		mqttc.tls_set(args.mqtt_cafile, cert_reqs=ssl.CERT_REQUIRED)
	mqttc.tls_insecure_set(args.mqtt_insecure)

# username & password may be None
if args.mqtt_username is not None:
	mqttc.username_pw_set(args.mqtt_username, args.mqtt_password)

# Attempt to connect to broker. If this fails, issue CRITICAL
try:
	mqttc.connect(args.mqtt_host, args.mqtt_port, 60)
except Exception, e:
	message = "Connection to %s:%d failed: %s" % (args.mqtt_host, args.mqtt_port, str(e))
	exitus(3, message)

rc = 0
while rc == 0:
	rc = mqttc.loop()

# disconnect from server
exit(rc,"MQTT disconnected")
