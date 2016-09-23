"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import ConfigParser
import logging
import pkg_resources

from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from webservice.NexusHandler import nexus_initializer


@nexus_initializer
class DomsInitializer:
    def __init__(self):
        pass

    def init(self, config):
        log = logging.getLogger(__name__)
        log.info("*** STARTING DOMS INITIALIZATION ***")

        domsconfig = ConfigParser.RawConfigParser()
        domsconfig.readfp(pkg_resources.resource_stream(__name__, "domsconfig.ini"), filename='domsconfig.ini')

        cassHost = domsconfig.get("cassandra", "host")
        cassKeyspace = domsconfig.get("cassandra", "keyspace")
        cassDatacenter = domsconfig.get("cassandra", "local_datacenter")
        cassVersion = int(domsconfig.get("cassandra", "protocol_version"))

        log.info("Cassandra Host(s): %s" % (cassHost))
        log.info("Cassandra Keyspace: %s" % (cassKeyspace))
        log.info("Cassandra Datacenter: %s" % (cassDatacenter))
        log.info("Cassandra Protocol Version: %s" % (cassVersion))

        dc_policy = DCAwareRoundRobinPolicy(cassDatacenter)
        token_policy = TokenAwarePolicy(dc_policy)

        with Cluster([host for host in cassHost.split(',')], load_balancing_policy=token_policy,
                     protocol_version=cassVersion) as cluster:
            session = cluster.connect()

            self.createKeyspace(session, cassKeyspace)
            self.createTables(session)

    def createKeyspace(self, session, cassKeyspace):
        log = logging.getLogger(__name__)
        log.info("Verifying DOMS keyspace '%s'" % cassKeyspace)
        session.execute(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };" % cassKeyspace)
        session.set_keyspace(cassKeyspace)

    def createTables(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying DOMS tables")
        self.createDomsExecutionsTable(session)
        self.createDomsParamsTable(session)
        self.createDomsDataTable(session)
        self.createDomsExecutionStatsTable(session)

    def createDomsExecutionsTable(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying doms_executions table")
        cql = """
            CREATE TABLE IF NOT EXISTS doms_executions (
              id text PRIMARY KEY,
              time_started bigint,
              time_completed bigint,
              user_email text
            );
                """
        session.execute(cql)

    def createDomsParamsTable(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying doms_params table")
        cql = """
            CREATE TABLE IF NOT EXISTS doms_params (
              execution_id text PRIMARY KEY,
              primary_dataset text,
              matchup_datasets text,
              depth_tolerance float,
              time_tolerance int,
              radius_tolerance float,
              start_time bigint,
              end_time bigint,
              platforms text,
              bounding_box text
            );
        """
        session.execute(cql)

    def createDomsDataTable(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying doms_data table")
        cql = """
            CREATE TABLE IF NOT EXISTS doms_data (
              id text PRIMARY KEY,
              execution_id text,
              value_id text,
              primary_value_id text,
              is_primary int,
              x float,
              y float,
              source_dataset text,
              measurement_time bigint,
              platform text,
              device text,
              measurement_values map<text, float>
            );
        """
        session.execute(cql)
        session.execute("CREATE INDEX IF NOT EXISTS  doms_data_execution_id ON doms_data( execution_id );")
        session.execute("CREATE INDEX IF NOT EXISTS  doms_data_is_primary ON doms_data( is_primary );")

    def createDomsExecutionStatsTable(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying doms_execuction_stats table")
        cql = """
            CREATE TABLE IF NOT EXISTS doms_execution_stats (
              execution_id text PRIMARY KEY,
              num_gridded_matched int,
              num_gridded_checked int,
              num_insitu_matched int,
              num_insitu_checked int,
              time_to_complete int
            );
        """
        session.execute(cql)
