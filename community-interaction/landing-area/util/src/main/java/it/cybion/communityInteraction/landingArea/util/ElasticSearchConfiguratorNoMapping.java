package it.cybion.communityInteraction.landingArea.util;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexMissingException;

/**
 * Simple class to configure ElasticSearch started as a service
 */
public class ElasticSearchConfiguratorNoMapping 
{
    public static void main( String[] args ) throws InterruptedException{
    	
    	final String host = "localhost";
        final int transportPort = 9300;
        
        
        final String clusterName = "community-interaction-development";
        final String index = "twitter-champions";
        
        
     // Create a TransportClient
        final Settings transportClientSettings = ImmutableSettings.settingsBuilder().put(
                "cluster.name", clusterName).build();
        Client transportClient = new TransportClient(transportClientSettings).addTransportAddress(
                new InetSocketTransportAddress(host, transportPort));
        
        transportClient.admin()
        			   .cluster()
        			   .prepareHealth()
        			   .setWaitForYellowStatus()
        			   .execute()
        			   .actionGet();
                
        try {
        	DeleteIndexResponse deleteIndexResponse = transportClient.admin()
            									 .indices()
            									 .delete(new DeleteIndexRequest(index))
            									 .actionGet();
        } catch (IndexMissingException e) {
            System.out.println("index does not exist - continue");
        }

		finally {
	    	transportClient.admin()
	    				   .indices()
	    				   .prepareCreate(index)
	    				   .execute()
	    				   .actionGet();
		}
    }
    
    public static Settings buildNodeSettings(final String host, final String port,
            final String transportPort, final String clusterName, final String elasticSearchPath) {

        return ImmutableSettings.settingsBuilder().put("network.host", host).put("http.port", port)
                .put("transport.tcp.port", transportPort).put("cluster.name", clusterName)
                .put("path.data", elasticSearchPath + "data")
                .put("path.logs", elasticSearchPath + "logs")
                .put("path.work", elasticSearchPath + "work").build();
    }

}
