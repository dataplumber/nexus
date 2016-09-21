"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import ConfigParser
import logging
import pkg_resources
from os import environ

__log = logging.getLogger(__name__)
# Load Cassandra and Solr environment properties
__config = ConfigParser.SafeConfigParser()
__config_paths = ['/etc/nexus_data_access/datastores.ini',
                  '~/.nexus_data_access/datastores.ini']

__default_config = ConfigParser.RawConfigParser()
__default_config.readfp(pkg_resources.resource_stream(__name__, "config/default-datastores.ini"),
                        filename='default-datastores.ini')

__configs_read = __config.read(__config_paths)
if len(__configs_read) > 0:
    __log.debug("Successfully read config from %s" % __configs_read)
else:
    __log.debug("Could not find config file. Tried %s" % __config_paths)

DATASTORE_CONFIG = {}

__CASSANDRA_CONTACT_POINTS = 'CASSANDRA_CONTACT_POINTS'
try:
    DATASTORE_CONFIG[__CASSANDRA_CONTACT_POINTS] = environ[__CASSANDRA_CONTACT_POINTS]
    __log.debug("Set %s from environment variable" % __CASSANDRA_CONTACT_POINTS)
except KeyError:
    try:
        DATASTORE_CONFIG[__CASSANDRA_CONTACT_POINTS] = __config.get("cassandra", "contact_points")
        __log.debug("Set %s from configuration file" % __CASSANDRA_CONTACT_POINTS)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        try:
            DATASTORE_CONFIG[__CASSANDRA_CONTACT_POINTS] = __default_config.get("cassandra", "contact_points")
            __log.debug("Set %s from default configuration file" % __CASSANDRA_CONTACT_POINTS)
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            raise EnvironmentError("Environment variable %s is required" % __CASSANDRA_CONTACT_POINTS)

__CASSANDRA_KEYSPACE = 'CASSANDRA_KEYSPACE'
try:
    DATASTORE_CONFIG[__CASSANDRA_KEYSPACE] = environ[__CASSANDRA_KEYSPACE]
    __log.debug("Set %s from environment variable" % __CASSANDRA_KEYSPACE)
except KeyError:
    try:
        DATASTORE_CONFIG[__CASSANDRA_KEYSPACE] = __config.get("cassandra", "keyspace")
        __log.debug("Set %s from configuration file" % __CASSANDRA_KEYSPACE)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        try:
            DATASTORE_CONFIG[__CASSANDRA_KEYSPACE] = __default_config.get("cassandra", "keyspace")
            __log.debug("Set %s from default configuration file" % __CASSANDRA_KEYSPACE)
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            raise EnvironmentError("Environment variable %s is required" % __CASSANDRA_KEYSPACE)

__CASSANDRA_LOCAL_DATACENTER = 'CASSANDRA_LOCAL_DATACENTER'
try:
    DATASTORE_CONFIG[__CASSANDRA_LOCAL_DATACENTER] = environ[__CASSANDRA_LOCAL_DATACENTER]
    __log.debug("Set %s from environment variable" % __CASSANDRA_LOCAL_DATACENTER)
except KeyError:
    try:
        DATASTORE_CONFIG[__CASSANDRA_LOCAL_DATACENTER] = __config.get("cassandra", "local_datacenter")
        __log.debug("Set %s from configuration file" % __CASSANDRA_LOCAL_DATACENTER)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        try:
            DATASTORE_CONFIG[__CASSANDRA_LOCAL_DATACENTER] = __default_config.get("cassandra", "local_datacenter")
            __log.debug("Set %s from default configuration file" % __CASSANDRA_LOCAL_DATACENTER)
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            raise EnvironmentError("Environment variable %s is required" % __CASSANDRA_LOCAL_DATACENTER)

__CASSANDRA_PROTOCOL_VERSION = 'CASSANDRA_PROTOCOL_VERSION'
try:
    DATASTORE_CONFIG[__CASSANDRA_PROTOCOL_VERSION] = environ[__CASSANDRA_PROTOCOL_VERSION]
    __log.debug("Set %s from environment variable" % __CASSANDRA_PROTOCOL_VERSION)
except KeyError:
    try:
        DATASTORE_CONFIG[__CASSANDRA_PROTOCOL_VERSION] = __config.get("cassandra", "protocol_version")
        __log.debug("Set %s from configuration file" % __CASSANDRA_PROTOCOL_VERSION)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        try:
            DATASTORE_CONFIG[__CASSANDRA_PROTOCOL_VERSION] = __default_config.get("cassandra", "protocol_version")
            __log.debug("Set %s from default configuration file" % __CASSANDRA_PROTOCOL_VERSION)
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            raise EnvironmentError("Environment variable %s is required" % __CASSANDRA_PROTOCOL_VERSION)

__SOLR_HOST = 'SOLR_HOST'
try:
    DATASTORE_CONFIG[__SOLR_HOST] = environ[__SOLR_HOST]
    __log.debug("Set %s from environment variable" % __SOLR_HOST)
except KeyError:
    try:
        DATASTORE_CONFIG[__SOLR_HOST] = __config.get("solr", "host")
        __log.debug("Set %s from configuration file" % __SOLR_HOST)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        try:
            DATASTORE_CONFIG[__SOLR_HOST] = __default_config.get("solr", "host")
            __log.debug("Set %s from default configuration file" % __SOLR_HOST)
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            raise EnvironmentError("Environment variable %s is required" % __SOLR_HOST)

__SOLR_CORE = 'SOLR_CORE'
try:
    DATASTORE_CONFIG[__SOLR_CORE] = environ[__SOLR_CORE]
    __log.debug("Set %s from environment variable" % __SOLR_CORE)
except KeyError:
    try:
        DATASTORE_CONFIG[__SOLR_CORE] = __config.get("solr", "core")
        __log.debug("Set %s from configuration file" % __SOLR_CORE)
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        try:
            DATASTORE_CONFIG[__SOLR_CORE] = __default_config.get("solr", "core")
            __log.debug("Set %s from default configuration file" % __SOLR_CORE)
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            raise EnvironmentError("Environment variable %s is required" % __SOLR_CORE)

CASSANDRA_CONTACT_POINTS = DATASTORE_CONFIG[__CASSANDRA_CONTACT_POINTS]
CASSANDRA_KEYSPACE = DATASTORE_CONFIG[__CASSANDRA_KEYSPACE]
CASSANDRA_LOCAL_DATACENTER = DATASTORE_CONFIG[__CASSANDRA_LOCAL_DATACENTER]
CASSANDRA_PROTOCOL_VERSION = DATASTORE_CONFIG[__CASSANDRA_PROTOCOL_VERSION]
SOLR_HOST = DATASTORE_CONFIG[__SOLR_HOST]
SOLR_CORE = DATASTORE_CONFIG[__SOLR_CORE]
