package it.cybion.communityInteraction.landingArea.util;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple class to configure ElasticSearch started as a service
 */
public class ElasticsearchImportExport {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchImportExport.class);
    
    
    public static void main( String[] args ) throws InterruptedException{
    	
    	final int maxTweets = 100000;
        final int scrollSize = 500;
        final int shards = 3;
        final int realTotal = (int) Math.ceil((double)maxTweets /(scrollSize * shards))*scrollSize * shards;
        
    	final String localHost = "localhost";
        final int localTransportPort = 9300;
        final String localClusterName = "community-interaction-development";
        final String localIndex = "twitter-champions";
        final String localType = "tweet";
        
        final String gaiaHost = "gaia.cybion.eu";
        final int gaiaTransportPort = 9300;
        final String gaiaClusterName = "social-insights";
        final String gaiaIndex = "twitter_v1";
        final String gaiaType = "context_tweet‏";
        final String twitterChampion = "ee7eb7f6-8ab1-40ad-b7f8-d47fe0ae02a0";
        
        // Create a TransportClient for localhost
        Client localTransportClient = buildClient(localHost, localTransportPort, localClusterName);
        
        localTransportClient.admin()
	        			    .cluster()
	        			    .prepareHealth()
	        			    .setWaitForYellowStatus()
	        			    .execute()
	        			    .actionGet();
        
        // Create a TransportClient for gaia
        Client gaiaTransportClient = buildClient(gaiaHost, gaiaTransportPort, gaiaClusterName);
        
        final QueryBuilder query = QueryBuilders.boolQuery().must(
                QueryBuilders.fieldQuery("monitoringActivityId", twitterChampion));
        
        SearchResponse scrollResp = gaiaTransportClient.prepareSearch(gaiaIndex).setTypes("context_tweet")
                .setSearchType(SearchType.SCAN).setScroll(new TimeValue(60000)).setQuery(query)
                .addSort(SortBuilders.fieldSort("tweet.createdAt").order(SortOrder.DESC))
                .setSize(scrollSize).execute().actionGet();

        
        // Scroll until no hits are returned
        int count = 0;
    	LOGGER.info("Start");
        while (count < maxTweets) {
            scrollResp = gaiaTransportClient.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(600000)).execute().actionGet();
            for (SearchHit hit : scrollResp.getHits()) {

                try {
                	storeTweet(localTransportClient, localIndex, localType, hit.getSourceAsString());
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                    continue;
                }
            }
            count += scrollResp.getHits().getHits().length;
        	LOGGER.info(count + " / " + realTotal);
            // Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
    	LOGGER.info("End");
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
