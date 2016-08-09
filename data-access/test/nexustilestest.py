"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import pyximport

pyximport.install()

from nexustiles.nexustiles import NexusTileService

service = NexusTileService()

assert service is not None

# tiles = service.find_tile_by_id('7d859bcc-6dc6-3581-9ac9-7e6e52491670')
#
# assert len(tiles) == 1
#
# tile = tiles[0]
# assert tile.meta_data is not None
#
# print tile.get_summary()
