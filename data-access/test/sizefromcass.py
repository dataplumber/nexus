"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import ConfigParser

import pkg_resources

from nexustiles.dao.CassandraProxy import CassandraProxy

config = ConfigParser.RawConfigParser()

config.readfp(pkg_resources.resource_stream(__name__, "config/datastores.ini"), filename='datastores.ini')

cass = CassandraProxy(config)

tiles = cass.fetch_nexus_tiles('aec174e2-64e4-3602-82a9-aa3f530007a9')
print len(tiles[0].tile_blob)
