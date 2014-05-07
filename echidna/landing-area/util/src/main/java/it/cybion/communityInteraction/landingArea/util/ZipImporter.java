package it.cybion.communityInteraction.landingArea.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.codehaus.jettison.json.JSONArray;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


/** Simple class to write to Elasticsearch the context_tweet data retrieved from a zip file */
public class ZipImporter {
	
    public static void main(String args[]) throws Exception {
        
    	if(args.length < 1) {
        	throw new Exception("Zipfile name file missing");
        }
 
        final InputStream file = new FileInputStream(args[0]);
        final ZipInputStream stream = new ZipInputStream(file);
                
    	final String localHost = "localhost";
        final int localTransportPort = 9300;
        final String localClusterName = "community-interaction-development";
        final String localIndex = "twitter-champions";
        final String localType = "context_tweet";
        final Client client = buildClient(localHost, localTransportPort, localClusterName);
 
        int counter = 0;
		System.out.println("Start indexing the context_tweets from zip file");
        try {
	        ZipEntry entry;
	        while (((entry = stream.getNextEntry())) != null) {
	                
	        	StringBuilder sb = new StringBuilder();
	            for (int c = stream.read(); c != -1; c = stream.read()) {
	                sb.append((char) c);
	            }
	            JSONArray tweetJsons = new JSONArray(new String(sb.toString()));
	            
	            for(int i=0; i<tweetJsons.length(); i++) {
	        		storeTweet(client, localIndex, localType, tweetJsons.getString(i));
	        		counter++;
	        		System.out.println("Stored document number: " + counter);
	            }
	        }
        }
        finally {
            stream.close();
        }
		System.out.println("Total number of context_tweet documents indexed: " + counter);
    }
    
    private static Client buildClient(final String host, final int port, final String clusterName) {
    	
    	final Settings transportClientSettings = ImmutableSettings.settingsBuilder().put(
                "cluster.name", clusterName).build();
        return new TransportClient(transportClientSettings).addTransportAddress(
                new InetSocketTransportAddress(host, port));
    }
    
    private static void storeTweet(Client client, String index, String type, String tweet) {
    	
    	final IndexResponse response = client.prepareIndex(index, type)
							                 .setSource(tweet)
							                 .setRefresh(true)
							                 .execute()
							                 .actionGet();
    }
}