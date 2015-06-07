#!/bin/bash
# @author Robin Verlangen
# This script takes all steps needed to setup rsyslog to forward data directly to a kafka topic
KAFKA_TOPIC=$1
KAFKA_BROKERS=$2
if [ -z "$KAFKA_TOPIC" ]; then
	echo "Param 1 kafka topic not set"
	exit 1
fi
if [ -z "$KAFKA_BROKERS" ]; then
	echo "Param 2 kafka brokers (host1:port,host2:port) not set"
	exit 1
fi

# Setup rsyslog repo
if [ ! -f /etc/yum.repos.d/rsyslog.repo ]; then
	echo -e "[rsyslog_v8]\nname=Adiscon CentOS-\$releasever - local packages for \$basearch\nbaseurl=http://rpms.adiscon.com/v8-stable/epel-\$releasever/\$basearch\nenabled=1\ngpgcheck=0\ngpgkey=http://rpms.adiscon.com/RPM-GPG-KEY-Adiscon\nprotect=1" > /etc/yum.repos.d/rsyslog.repo
fi

# Install
yum -y install rsyslog rsyslog-kafka

# Config rsyslog kafka
if [ ! -f /etc/rsyslog.d/kafka.conf ]; then
	echo -e "module(load=\"omkafka\")\naction(type=\"omkafka\" topic=\"$KAFKA_TOPIC\" confParam=[\"compression.codec=snappy\"] broker=\"$KAFKA_BROKERS\")" > /etc/rsyslog.d/kafka.conf
fi

# Validate config
rsyslogd -N1
