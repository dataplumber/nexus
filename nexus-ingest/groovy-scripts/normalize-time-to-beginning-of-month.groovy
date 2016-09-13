/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
/**
 * Created by greguska on 3/29/16.
 */

@Grapes([
    @Grab(group = 'org.nasa.jpl.nexus', module = 'nexus-messages', version = '1.0.0.RELEASE')
])

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent


if (!(payload instanceof byte[])){
    throw new RuntimeException("Can't handle messages that are not byte[]. Got payload of type ${payload.class}")
}

def tileBuilder = NexusContent.NexusTile.newBuilder().mergeFrom(payload)

switch(tileBuilder.tile.tileTypeCase){
    case NexusContent.TileData.TileTypeCase.GRID_TILE:
        def gridtilebuilder = tileBuilder.tileBuilder.gridTileBuilder
        def tilesummary = tileBuilder.summaryBuilder

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        currMinTime = tilesummary.statsBuilder.minTime
        currMaxTime = tilesummary.statsBuilder.maxTime

        def currMinTime = new Date(tilesummary.statsBuilder.minTime * 1000)
        def currMaxTime = new Date(tilesummary.statsBuilder.maxTime * 1000)

        currMinTime.set(date: 1, hourOfDay: 0)
        currMaxTime.set(date: 1, hourOfDay: 0)

        tilesummary.statsBuilder.setMinTime((currMinTime.time / 1000).toLong())
        tilesummary.statsBuilder.setMaxTime((currMaxTime.time / 1000).toLong())
        break
    default:
        throw new RuntimeException("Can only handle GridTile at this time.")
}

return tileBuilder.build().toByteArray()
