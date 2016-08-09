/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
package org.nasa.jpl.nexus.ingest.nexussink

import org.apache.commons.lang.NotImplementedException
import org.apache.solr.client.solrj.request.AbstractUpdateRequest
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.SolrInputField
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent.GridTile
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent.NexusTile
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent.TileSummary
import org.springframework.core.env.Environment
import org.springframework.data.cassandra.core.CassandraOperations
import org.springframework.data.solr.core.SolrOperations

import javax.annotation.Resource
import java.nio.ByteBuffer
import java.text.SimpleDateFormat

/**
 * Created by greguska on 4/4/16.
 */
class NexusService {

    @Resource
    private Environment environment;

    private static final def iso = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
    static{
        iso.setTimeZone(TimeZone.getTimeZone("UTC"))
    }

    private SolrOperations solr
    private CassandraOperations cassandraTemplate

    //TODO This will be refactored at some point to be dynamic per-message. Or maybe per-group.
    private String tableName="sea_surface_temp"

    public NexusService(SolrOperations solr, CassandraOperations cassandraTemplate) {
        this.solr = solr
        this.cassandraTemplate = cassandraTemplate
    }

    def saveToNexus(Collection<NexusTile> nexusTiles) {

        def solrdocs = nexusTiles.collect { nexusTile -> getSolrDocFromTileSummary(nexusTile.summary)}
        solr.saveDocuments(solrdocs, environment.getProperty("solrCommitWithin", Integer.class, 1000))

        def query = "insert into ${tableName} (tile_id, tile_blob) VALUES (?, ?)"
        cassandraTemplate.ingest(query, nexusTiles.collect{ nexusTile -> getCassandraRowFromTileData(nexusTile.tile)})

    }

    def getSolrDocFromTileSummary(TileSummary summary) {

        def bbox = summary.getBbox()
        def stats = summary.getStats()

        def startCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        startCal.setTime(new Date(stats.minTime*1000))
        def endCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        endCal.setTime(new Date(stats.maxTime*1000))

        def minTime = iso.format(startCal.getTime())
        def maxTime = iso.format(endCal.getTime())

        def geo = "POLYGON((" +
                "${bbox.lonMin} ${bbox.latMin}, " +
                "${bbox.lonMax} ${bbox.latMin}, " +
                "${bbox.lonMax} ${bbox.latMax}, " +
                "${bbox.lonMin} ${bbox.latMax}, " +
                "${bbox.lonMin} ${bbox.latMin}))"

        def doc = [
                "table_s"         : tableName,
                "geo"             : geo,
                "id"              : "$summary.tileId".toString(),
                "dataset_id_s"    : "$summary.datasetUuid".toString(),
                "sectionSpec_s"   : "$summary.sectionSpec".toString(),
                "dataset_s"       : "$summary.datasetName".toString(),
                "granule_s"       : "$summary.granule".toString(),
                "tile_var_name_s" : "$summary.dataVarName".toString(),
                "tile_min_lon"    : bbox.lonMin,
                "tile_max_lon"    : bbox.lonMax,
                "tile_min_lat"    : bbox.latMin,
                "tile_max_lat"    : bbox.latMax,
                "tile_min_time_dt": minTime,
                "tile_max_time_dt": maxTime,
                "tile_min_val_d"  : stats.min,
                "tile_max_val_d"  : stats.max,
                "tile_avg_val_d"  : stats.mean,
                "tile_count_i"    : stats.count
        ]

        summary.globalAttributesList.forEach { attribute ->
            doc["${attribute.name}"] = attribute.valuesCount==1?attribute.getValues(0):attribute.getValuesList().toList()
        }

        def solrdoc = toSolrInputDocument(doc)
        return solrdoc
    }

    def toSolrInputDocument(Map<String, Object> doc) {
        def solrDoc = new SolrInputDocument()
        solrDoc.putAll(doc.collectEntries { String key, Object value ->
            def field = new SolrInputField(key)
            field.setValue(value, 1.0f)
            [(key): field]
        })
        return solrDoc
    }

    def getCassandraRowFromTileData(NexusContent.TileData tile) {

        def tileId = UUID.fromString(tile.tileId)

        def row = [tileId, ByteBuffer.wrap(tile.toByteArray())]

        return row
    }

    public static void main(String... args){

    }
}
