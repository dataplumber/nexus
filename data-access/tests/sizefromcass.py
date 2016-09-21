"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import pyximport

pyximport.install()

from nexus.data_access.dao import CassandraProxy

cass = CassandraProxy()

tiles = cass.fetch_nexus_tiles('d9b5afe3-bd7f-3824-ad8a-d8d3b364689c')
print len(tiles[0].tile_blob)
