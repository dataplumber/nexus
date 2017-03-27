#!/bin/bash
 
# NOTE: This requires GNU getopt.  On Mac OS X and FreeBSD, you have to install this
# separately; see below.
TEMP=`getopt -o sch --long singleNode,container,help -n 'nexus-ingest' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

SINGLENODE=false
CONTAINER=false
while true; do
  case "$1" in
    -s | --singleNode ) SINGLENODE=true; shift ;;
    -c | --container ) CONTAINER=true; shift ;;
    -h | --help ) 
        echo "usage: nexus-ingest [-s|--singleNode] [-c|--container]" >&2
        exit 2
        ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ "$SINGLENODE" = true ]; then
    source activate nexus-xd-python-modules
    xd-singlenode --hadoopDistro none
elif [ "$CONTAINER"  = true ]; then
    source activate nexus-xd-python-modules
    export SPRING_DATASOURCE_URL="jdbc:mysql://$MYSQL_PORT_3306_TCP_ADDR:$MYSQL_PORT_3306_TCP_PORT/xdjob"
    export SPRING_DATASOURCE_USERNAME=$MYSQL_USER
    export SPRING_DATASOURCE_PASSWORD=$MYSQL_PASSWORD
    export SPRING_DATASOURCE_DRIVERCLASSNAME="com.mysql.jdbc.Driver"

    export ZK_CLIENT_CONNECT=$ZOOKEEPER_CONNECT
    export ZK_NAMESPACE=$ZOOKEEPER_XD_CHROOT

    export SPRING_REDIS_HOST=$REDIS_ADDR
    export SPRING_REDIS_PORT=$REDIS_PORT
    
    # export XD_TRANSPORT="kafka"
    xd-container --hadoopDistro none
else
    echo "either -s or -c is required."
    echo "usage: nexus-ingest [-s|--singleNode] [-c|--container]" >&2
    exit 3
fi