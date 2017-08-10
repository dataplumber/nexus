package org.nasa.jpl.nexus.ingest.nexussink;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent;

import java.util.Collection;

/**
 * Created by djsilvan on 6/26/17.
 */
public class DynamoStore implements DataStore {

    private DynamoDB dynamoDB;
    private String tableName;
    private String primaryKey = "tile_id";
    private int count = 0;

    public DynamoStore(AmazonDynamoDB dynamoClient, String tableName) {
        dynamoDB = new DynamoDB(dynamoClient);
        this.tableName = tableName;
    }

    public void saveData(Collection<NexusContent.NexusTile> nexusTiles) {

        Table table = dynamoDB.getTable(tableName);

        for (NexusContent.NexusTile tile : nexusTiles) {
            String tileId = getTileId(tile);
            byte[] tileData = getTileData(tile);

            try {
                table.putItem(new Item().withPrimaryKey(primaryKey, tileId).withBinary("data", tileData));
                count++;
            }
            catch (Exception e) {
                System.err.println("Unable to add item: " + tileId);
                System.err.println(e.getMessage());
            }
        }
        if (count % 1053 == 0) {
            System.out.println("\nCOUNT (files): " + count/1053);
            System.out.println();
        }
    }

    private String getTileId(NexusContent.NexusTile tile) {
        return tile.getTile().getTileId();
    }

    private byte[] getTileData(NexusContent.NexusTile tile) {
        return tile.getTile().toByteArray();
    }
}
