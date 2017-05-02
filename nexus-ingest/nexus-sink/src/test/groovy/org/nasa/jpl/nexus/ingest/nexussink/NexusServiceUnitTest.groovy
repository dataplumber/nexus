package org.nasa.jpl.nexus.ingest.nexussink

import org.junit.Test
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent
import org.springframework.mock.env.MockEnvironment

import static org.junit.Assert.assertEquals

/**
 * Created by greguska on 5/2/17.
 */
class NexusServiceUnitTest {

    @Test
    public void testGetSolrDocFromTileSummary() {
        def sink = new NexusService(null, null)
        sink.setEnvironment(new MockEnvironment())


        def tileSummary = NexusContent.TileSummary.newBuilder()
                .setTileId("1")
                .setBbox(NexusContent.TileSummary.BBox.newBuilder()
                .setLatMin(51)
                .setLatMax(55)
                .setLonMin(22)
                .setLonMax(30)
                .build())
                .setDatasetName("test")
                .setDatasetUuid("4")
                .setDataVarName("sst")
                .setGranule("test.nc")
                .setSectionSpec("0:1,0:1")
                .setStats(NexusContent.TileSummary.DataStats.newBuilder()
                .setCount(10)
                .setMax(50)
                .setMin(50)
                .setMean(50)
                .setMaxTime(1429142399)
                .setMinTime(1429142399)
                .build())
                .build()

        def doc = sink.getSolrDocFromTileSummary(tileSummary)

        assertEquals("2015-04-15T23:59:59Z", doc.get("tile_min_time_dt").value)

        println doc
    }
}
