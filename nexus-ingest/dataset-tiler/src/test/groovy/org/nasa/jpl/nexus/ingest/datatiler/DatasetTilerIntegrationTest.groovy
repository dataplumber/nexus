/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
package org.nasa.jpl.nexus.ingest.datatiler

import org.junit.After
import org.junit.BeforeClass
import org.junit.Test
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.FileSystemResource
import org.springframework.xd.dirt.server.singlenode.SingleNodeApplication
import org.springframework.xd.dirt.test.SingleNodeIntegrationTestSupport
import org.springframework.xd.dirt.test.SingletonModuleRegistry
import org.springframework.xd.dirt.test.process.SingleNodeProcessingChain
import org.springframework.xd.module.ModuleType
import org.springframework.xd.test.RandomConfigurationSupport

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.springframework.xd.dirt.test.process.SingleNodeProcessingChainSupport.chain;

/**
 * Created by greguska on 3/1/16.
 */
class DatasetTilerIntegrationTest {

    private static SingleNodeApplication application;

    private static int RECEIVE_TIMEOUT = Integer.MAX_VALUE;

    private static String moduleName = "dataTiler";

    SingleNodeProcessingChain chain;

    /**
     * Start the single node container, binding random unused ports, etc. to not conflict with any other instances
     * running on this host. Configure the ModuleRegistry to include the project module.
     */
    @BeforeClass
    public static void setUp() {
        RandomConfigurationSupport randomConfigSupport = new RandomConfigurationSupport();
        application = new SingleNodeApplication().run();
        SingleNodeIntegrationTestSupport singleNodeIntegrationTestSupport = new SingleNodeIntegrationTestSupport
                (application);
        singleNodeIntegrationTestSupport.addModuleRegistry(new SingletonModuleRegistry(ModuleType.processor,
                moduleName));

    }

    @Test
    public void testSliceByDesiredTiles() {
        def tilesDesired = 1
        def dimensions = "lat,lon"
//        def splitResult = true

        def payload = new ClassPathResource("datasets/20150101090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc.nc4").getFile()

        def streamName = "testDefault"


        def processingChainUnderTest = "$moduleName --tilesDesired=$tilesDesired --dimensions=$dimensions"

        chain = chain(application, streamName, processingChainUnderTest)

        chain.sendPayload(payload);
        Object result = chain.receivePayload(RECEIVE_TIMEOUT)

        assertTrue(result instanceof List)
        assertEquals(1, result.size)
        assertEquals("lat:0:249,lon:0:500", result[0].toString())
    }

    @Test
    public void testSliceByDimension() {
        def dimensions = "NUMROWS,NUMCELLS"
        def sliceByDimension = "NUMROWS"

        def payload = new ClassPathResource("datasets/ascat_20130325_002100_metopb_02676_eps_o_coa_2101_ovw.l2.nc4").getFile()

        def streamName = "testDefault"


        def processingChainUnderTest = "$moduleName --sliceByDimension=$sliceByDimension --dimensions=$dimensions"

        chain = chain(application, streamName, processingChainUnderTest)

        chain.sendPayload(payload);
        Object result = chain.receivePayload(RECEIVE_TIMEOUT)

        assertTrue(result instanceof List)
        assertEquals(2, result.size)

        assertEquals("NUMROWS:0:1,NUMCELLS:0:82", result[0].toString())
        assertEquals("NUMROWS:1:2,NUMCELLS:0:82", result[1].toString())
    }

    @Test
    public void testSliceByDimensionSplitResult() {
        def dimensions = "NUMROWS,NUMCELLS"
        def sliceByDimension = "NUMROWS"
        def splitResult = true

        def payload = new ClassPathResource("datasets/ascat_20130325_002100_metopb_02676_eps_o_coa_2101_ovw.l2.nc4").getFile()

        def streamName = "testDefault"


        def processingChainUnderTest = "$moduleName --sliceByDimension=$sliceByDimension --dimensions=$dimensions --splitResult=$splitResult"

        chain = chain(application, streamName, processingChainUnderTest)

        chain.sendPayload(payload);
        Object result = chain.receivePayload(RECEIVE_TIMEOUT)

        assertTrue(result instanceof String)
        assertEquals("NUMROWS:0:1,NUMCELLS:0:82", result.toString())

        result = chain.receivePayload(RECEIVE_TIMEOUT)

        assertTrue(result instanceof String)
        assertEquals("NUMROWS:1:2,NUMCELLS:0:82", result.toString())
    }

    @Test
    public void testSliceByDimensionIndexedDimensions() {
        def dimensions = "0,1"
        def sliceByDimension = "1"

        def payload = new ClassPathResource("datasets/SMAP_L2B_SSS_00865_20150331T163144_R13080.split.h5").getFile()

        def streamName = "testDefault"


        def processingChainUnderTest = "$moduleName --sliceByDimension=$sliceByDimension --dimensions=$dimensions"

        chain = chain(application, streamName, processingChainUnderTest)

        chain.sendPayload(payload);
        Object result = chain.receivePayload(RECEIVE_TIMEOUT)

        assertTrue(result instanceof List)
        assertEquals(2, result.size)

        assertEquals("phony_dim_0:0:76,phony_dim_1:0:1", result[0].toString())
        assertEquals("phony_dim_0:0:76,phony_dim_1:1:2", result[1].toString())
    }

    /**
     * Destroy the chain to reset message bus bindings and destroy the stream.
     */
    @After
    public void tearDown() {
        if (chain != null) {
            chain.destroy();
        }
    }

}
