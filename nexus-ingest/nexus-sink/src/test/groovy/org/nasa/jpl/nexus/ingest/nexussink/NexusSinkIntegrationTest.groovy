/*****************************************************************************
 * Copyright (c) 2016 Jet Propulsion Laboratory,
 * California Institute of Technology.  All rights reserved
 *****************************************************************************/
package org.nasa.jpl.nexus.ingest.nexussink

import com.datastax.driver.core.Cluster
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.params.ModifiableSolrParams
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExternalResource
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent
import org.springframework.core.io.ClassPathResource
import org.springframework.data.cassandra.core.CassandraOperations
import org.springframework.data.cassandra.core.CassandraTemplate
import org.springframework.xd.dirt.plugins.ModuleConfigurationException
import org.springframework.xd.dirt.server.singlenode.SingleNodeApplication
import org.springframework.xd.dirt.test.SingleNodeIntegrationTestSupport
import org.springframework.xd.dirt.test.SingletonModuleRegistry
import org.springframework.xd.dirt.test.process.SingleNodeProcessingChainProducer
import org.springframework.xd.module.ModuleType
import org.springframework.xd.test.RandomConfigurationSupport

import java.util.function.Supplier

import static org.junit.Assert.*
import static org.nasa.jpl.nexus.ingest.nexussink.NexusSinkOptionsMetadata.*
import static org.springframework.xd.dirt.test.process.SingleNodeProcessingChainSupport.chainProducer

/**
 * Created by greguska on 4/4/16.
 */
public class NexusSinkIntegrationTest {

    private static final String MODULE_NAME = "nexus"

    private static SingleNodeApplication application


    private static final String CASSANDRA_CONFIG = "spring-cassandra.yaml"

    private static final String CASSANDRA_KEYSPACE = "testNexusSink"

    private static final int PORT = 9043 // See spring-cassandra.yaml - native_transport_port

    private static final String CONTACT = "127.0.0.1"

    private static Cluster cluster

    private static CassandraOperations cassandraTemplate

    private static String SOLR_URL = "http://embedded"
    private static String SOLR_CORE = "nexustiles"

    private static SingleNodeIntegrationTestSupport singleNodeIntegrationTestSupport

    @BeforeClass
    public static void setUpXd() {

        initCassandra()

        new RandomConfigurationSupport()

        RandomConfigurationSupport randomConfigSupport = new RandomConfigurationSupport();
        application = new SingleNodeApplication().run()

        singleNodeIntegrationTestSupport = new SingleNodeIntegrationTestSupport
                (application);
        singleNodeIntegrationTestSupport.addModuleRegistry(new SingletonModuleRegistry(ModuleType.sink,
                MODULE_NAME));

    }

    private static void initCassandra() {
        System.setProperty("log4j.configuration", new ClassPathResource("log4j-embedded-cassandra.properties").file.absolutePath)
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG, "build/embeddedCassandra")
        cluster = Cluster.builder()
                .addContactPoint(CONTACT)
                .withPort(PORT)
                .build()

        cluster.connect().execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s" +
                "  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", CASSANDRA_KEYSPACE))

        cassandraTemplate = new CassandraTemplate(cluster.connect(CASSANDRA_KEYSPACE));

        for (String statement : new ClassPathResource("init-db.cql").getFile().text.split(';')) {
            if ("".equals(statement.trim())) {
                continue
            }
            cassandraTemplate.execute(statement)
        }
    }

    @AfterClass
    public static void cleanup() {

        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()

    }

    @Rule
    public ExternalResource checkCassAndSolr = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            assertFalse cluster.isClosed()

            cassandraTemplate.query("truncate ${CASSANDRA_KEYSPACE}.sea_surface_temp;")

            try {
                new ClassPathResource("solr/nexustiles/data").getFile().deleteDir()
            } catch (FileNotFoundException e) {
                //Ignore it.
            }
        }
    }

    /**
     * Test a Nexus sink module
     */
    @Test
    public void testNexusSink() {

        def streamName = "testNexusSink"

        NexusContent.NexusTile tile = NexusContent.NexusTile.newBuilder()
                .setTile(NexusContent.TileData.newBuilder()
                .setTileId(UUID.randomUUID().toString())
                .setGridTile(
                NexusContent.GridTile.newBuilder()
                        .build())
                .build())
                .setSummary(NexusContent.TileSummary.newBuilder()
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
                .setMaxTime(500000)
                .setMinTime(500000)
                .build())
                .addGlobalAttributes(NexusContent.Attribute.newBuilder()
                .setName("day_of_year_i")
                .addValues("006")
                .build())
                .addGlobalAttributes(NexusContent.Attribute.newBuilder()
                .setName("attr_multi_value_test")
                .addValues("006")
                .addValues("multi")
                .build())
                .build())
                .build()


        def processingChainUnderTest = "$MODULE_NAME --$PROPERTY_NAME_CASSANDRA_CONTACT_POINTS=$CONTACT --$PROPERTY_NAME_CASSANDRA_KEYSPACE=$CASSANDRA_KEYSPACE --$PROPERTY_NAME_CASSANDRA_PORT=$PORT --$PROPERTY_NAME_SOLR_SERVER_URL=$SOLR_URL --$PROPERTY_NAME_SOLR_COLLECTION=$SOLR_CORE"

        SingleNodeProcessingChainProducer chain = chainProducer(application, streamName, processingChainUnderTest)
        SolrClient solrClient = singleNodeIntegrationTestSupport.getModule(streamName, MODULE_NAME, 0).applicationContext.getBean("solrClient", SolrClient)

        chain.sendPayload(tile)

        assertEqualsEventually 1, new Supplier<Integer>() {
            @Override
            Integer get() {
                solrClient.commit()
                def q = solrClient.query(new ModifiableSolrParams().add("q", "*:*"))
                println "query: $q"
                return q.results.numFound
            }
        }


        assertEqualsEventually 1, new Supplier<Integer>() {
            @Override
            Integer get() {
                cassandraTemplate.query("select * from sea_surface_temp;").getAvailableWithoutFetching()
            }
        }

        chain.destroy()

    }

    /**
     * Test a Nexus sink module
     */
    @Test
    public void testNexusSinkSingleCount() {

        def streamName = "testNexusSink"

        NexusContent.NexusTile tile = NexusContent.NexusTile.newBuilder()
                .setTile(NexusContent.TileData.newBuilder()
                .setTileId(UUID.randomUUID().toString())
                .setGridTile(
                NexusContent.GridTile.newBuilder()
                        .build())
                .build())
                .setSummary(NexusContent.TileSummary.newBuilder()
                .setTileId("1")
                .setBbox(NexusContent.TileSummary.BBox.newBuilder()
                .setLatMin(51)
                .setLatMax(51)
                .setLonMin(22)
                .setLonMax(22)
                .build())
                .setDatasetName("test")
                .setDatasetUuid("4")
                .setDataVarName("sst")
                .setGranule("test.nc")
                .setSectionSpec("0:1,0:1")
                .setStats(NexusContent.TileSummary.DataStats.newBuilder()
                .setCount(1)
                .setMax(50)
                .setMin(50)
                .setMean(50)
                .setMaxTime(500000)
                .setMinTime(500000)
                .build())
                .addGlobalAttributes(NexusContent.Attribute.newBuilder()
                .setName("day_of_year_i")
                .addValues("006")
                .build())
                .addGlobalAttributes(NexusContent.Attribute.newBuilder()
                .setName("attr_multi_value_test")
                .addValues("006")
                .addValues("multi")
                .build())
                .build())
                .build()


        def processingChainUnderTest = "$MODULE_NAME --$PROPERTY_NAME_CASSANDRA_CONTACT_POINTS=$CONTACT --$PROPERTY_NAME_CASSANDRA_KEYSPACE=$CASSANDRA_KEYSPACE --$PROPERTY_NAME_CASSANDRA_PORT=$PORT --$PROPERTY_NAME_SOLR_SERVER_URL=$SOLR_URL --$PROPERTY_NAME_SOLR_COLLECTION=$SOLR_CORE"

        SingleNodeProcessingChainProducer chain = chainProducer(application, streamName, processingChainUnderTest)
        SolrClient solrClient = singleNodeIntegrationTestSupport.getModule(streamName, MODULE_NAME, 0).applicationContext.getBean("solrClient", SolrClient)

        chain.sendPayload(tile)

        assertEqualsEventually 0, new Supplier<Integer>() {
            @Override
            Integer get() {
                solrClient.commit()
                def q = solrClient.query(new ModifiableSolrParams().add("q", "*:*"))
                println "query: $q"
                return q.results.numFound
            }
        }


        assertEqualsEventually 0, new Supplier<Integer>() {
            @Override
            Integer get() {
                cassandraTemplate.query("select * from sea_surface_temp;").getAvailableWithoutFetching()
            }
        }

        chain.destroy()

    }


    @Test
    public void testAggregatingNexusSink() {

        def streamName = "testAggregatingNexusSink"

        def buffer = 5

        def processingChainUnderTest = "$MODULE_NAME --$PROPERTY_NAME_CASSANDRA_CONTACT_POINTS=$CONTACT --$PROPERTY_NAME_CASSANDRA_KEYSPACE=$CASSANDRA_KEYSPACE --$PROPERTY_NAME_CASSANDRA_PORT=$PORT --$PROPERTY_NAME_SOLR_SERVER_URL=$SOLR_URL --$PROPERTY_NAME_SOLR_COLLECTION=$SOLR_CORE --$PROPERTY_NAME_INSERT_BUFFER=$buffer"

        SingleNodeProcessingChainProducer chain = chainProducer(application, streamName, processingChainUnderTest)

        SolrClient solrClient = singleNodeIntegrationTestSupport.getModule(streamName, MODULE_NAME, 0).applicationContext.getBean("solrClient", SolrClient)

        for (x in 1..buffer * 2) {
            def tile = NexusContent.NexusTile.newBuilder()
                    .setTile(NexusContent.TileData.newBuilder()
                    .setTileId(UUID.randomUUID().toString())
                    .build())
                    .setSummary(NexusContent.TileSummary.newBuilder()
                    .setTileId("$x")
                    .setBbox(NexusContent.TileSummary.BBox.newBuilder()
                    .setLatMin(51)
                    .setLatMax(55)
                    .setLonMin(22)
                    .setLonMax(30)
                    .build())
                    .build())
                    .build()
            chain.sendPayload(tile)
        }

        assertEqualsEventually buffer * 2, new Supplier<Integer>() {
            @Override
            Integer get() {
                solrClient.commit()
                def q = solrClient.query(new ModifiableSolrParams().add("q", "*:*"))
                println "query: $q"
                return q.results.numFound
            }
        }


        assertEqualsEventually buffer * 2, new Supplier<Integer>() {
            @Override
            Integer get() {
                cassandraTemplate.query("select * from sea_surface_temp;").getAvailableWithoutFetching()
            }
        }

        chain.destroy()
    }

    @Test
    public void testByteConversion() {

        def streamName = "testByteConversion"

        def processingChainUnderTest = "$MODULE_NAME --$PROPERTY_NAME_CASSANDRA_CONTACT_POINTS=$CONTACT --$PROPERTY_NAME_CASSANDRA_KEYSPACE=$CASSANDRA_KEYSPACE --$PROPERTY_NAME_CASSANDRA_PORT=$PORT --$PROPERTY_NAME_SOLR_SERVER_URL=$SOLR_URL --$PROPERTY_NAME_SOLR_COLLECTION=$SOLR_CORE"

        SingleNodeProcessingChainProducer chain = chainProducer(application, streamName, processingChainUnderTest)

        SolrClient solrClient = singleNodeIntegrationTestSupport.getModule(streamName, MODULE_NAME, 0).applicationContext.getBean("solrClient", SolrClient)

        def tile = NexusContent.NexusTile.newBuilder()
                .setTile(NexusContent.TileData.newBuilder()
                .setTileId(UUID.randomUUID().toString())
                .build())
                .setSummary(NexusContent.TileSummary.newBuilder()
                .setTileId("1")
                .setBbox(NexusContent.TileSummary.BBox.newBuilder()
                .setLatMin(51)
                .setLatMax(55)
                .setLonMin(22)
                .setLonMax(30)
                .build())
                .build())
                .build()

        def tilebytes = tile.toByteArray()

        chain.sendPayload(tilebytes)

        assertEqualsEventually 1, new Supplier<Integer>() {
            @Override
            Integer get() {
                solrClient.commit()
                def q = solrClient.query(new ModifiableSolrParams().add("q", "*:*"))
                println "query: $q"
                return q.results.numFound
            }
        }


        assertEqualsEventually 1, new Supplier<Integer>() {
            @Override
            Integer get() {
                cassandraTemplate.query("select * from sea_surface_temp;").getAvailableWithoutFetching()
            }
        }

        chain.destroy()
    }

    @Test
    public void testByteConversionAggregation() {

        def streamName = "testByteConversionAggregation"
        def buffer = 2

        def processingChainUnderTest = "$MODULE_NAME --$PROPERTY_NAME_CASSANDRA_CONTACT_POINTS=$CONTACT --$PROPERTY_NAME_CASSANDRA_KEYSPACE=$CASSANDRA_KEYSPACE --$PROPERTY_NAME_CASSANDRA_PORT=$PORT --$PROPERTY_NAME_SOLR_SERVER_URL=$SOLR_URL --$PROPERTY_NAME_SOLR_COLLECTION=$SOLR_CORE --$PROPERTY_NAME_INSERT_BUFFER=$buffer"

        SingleNodeProcessingChainProducer chain = chainProducer(application, streamName, processingChainUnderTest)

        SolrClient solrClient = singleNodeIntegrationTestSupport.getModule(streamName, MODULE_NAME, 0).applicationContext.getBean("solrClient", SolrClient)

        def tile1 = NexusContent.NexusTile.newBuilder()
                .setTile(NexusContent.TileData.newBuilder()
                .setTileId(UUID.randomUUID().toString())
                .build())
                .setSummary(NexusContent.TileSummary.newBuilder()
                .setTileId("1")
                .setBbox(NexusContent.TileSummary.BBox.newBuilder()
                .setLatMin(51)
                .setLatMax(55)
                .setLonMin(22)
                .setLonMax(30)
                .build())
                .build())
                .build()

        def tile2 = NexusContent.NexusTile.newBuilder()
                .setTile(NexusContent.TileData.newBuilder()
                .setTileId(UUID.randomUUID().toString())
                .build())
                .setSummary(NexusContent.TileSummary.newBuilder()
                .setTileId("2")
                .setBbox(NexusContent.TileSummary.BBox.newBuilder()
                .setLatMin(51)
                .setLatMax(55)
                .setLonMin(22)
                .setLonMax(30)
                .build())
                .build())
                .build()

        chain.sendPayload(tile1.toByteArray())
        chain.sendPayload(tile2.toByteArray())

        assertEqualsEventually 2, new Supplier<Integer>() {
            @Override
            Integer get() {
                solrClient.commit()
                def q = solrClient.query(new ModifiableSolrParams().add("q", "*:*"))
                println "query: $q"
                return q.results.numFound
            }
        }


        assertEqualsEventually 2, new Supplier<Integer>() {
            @Override
            Integer get() {
                cassandraTemplate.query("select * from sea_surface_temp;").getAvailableWithoutFetching()
            }
        }

        chain.destroy()
    }

    @Test(expected = ModuleConfigurationException.class)
    public void testSolrZkAndSolrURLMutalExclusive() throws Exception {

        def streamName = "testSolrZkAndSolrURLMutalExclusive"

        def processingChainUnderTest = "$MODULE_NAME --$PROPERTY_NAME_SOLR_SERVER_URL=$SOLR_URL --$PROPERTY_NAME_SOLR_CLOUD_ZK_URL=zk1 --$PROPERTY_NAME_CASSANDRA_CONTACT_POINTS=$CONTACT --$PROPERTY_NAME_CASSANDRA_KEYSPACE=$CASSANDRA_KEYSPACE --$PROPERTY_NAME_CASSANDRA_PORT=$PORT --$PROPERTY_NAME_SOLR_COLLECTION=$SOLR_CORE"

        assertNull(chainProducer(application, streamName, processingChainUnderTest))
    }

    @Test()
    public void testSolrZk() throws Exception {

        def streamName = "testSolrZk"

        def processingChainUnderTest = "$MODULE_NAME --$PROPERTY_NAME_SOLR_CLOUD_ZK_URL=zkhost1:2181/solr --$PROPERTY_NAME_CASSANDRA_CONTACT_POINTS=$CONTACT --$PROPERTY_NAME_CASSANDRA_KEYSPACE=$CASSANDRA_KEYSPACE --$PROPERTY_NAME_CASSANDRA_PORT=$PORT --$PROPERTY_NAME_SOLR_COLLECTION=$SOLR_CORE"

        def chain = chainProducer(application, streamName, processingChainUnderTest)
        assertNotNull(chain)

        chain.destroy()
    }


    private static <T> void assertEqualsEventually(T expected, Supplier<T> actualSupplier) throws InterruptedException {
        int n = 0;
        while (!actualSupplier.get().equals(expected) && n++ < 100) {
            Thread.sleep(100);
        }
        assertTrue(n < 10);
    }

}
