package org.coprocessors;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Interface to define the methods an AuthorAggregatorEndpoint must implement
 * @author Daniele Morgantini
 */
public interface AuthorAggregatorProtocol extends CoprocessorProtocol {

	/** Method to compute the aggregation of the number of mention by mentioner and mentioned in the specified time range: Map<mentioned,Map<mentioner,times>> 
	 * @param auths the IDs of the users to filter the cells (if you only want the mentiones from these users)
	 * @params args the params in a map containing the id of the mentioned user and a list of the first and last row key to scan
	 * @return a Map<mentioned,Map<mentioner,times>> object
	 * @throws IOException */
	Map<String,Map<String,Integer>> aggregateMentionsByMM(final byte[][] auths, final Map<Long,List<String>> args) throws IOException;

	/** Method to compute the aggregation of the number of mention by mentioner and mentioned in this year's time range: Map<mentioned,Map<mentioner,times>> 
	 * @param auths the IDs of the users to filter the cells (if you only want the mentiones from these users)
	 * @params args the params in a map containing the id of the mentioned user and a list of the first and last row key to scan
	 * @return a Map<mentioned,Map<mentioner,times>> object
	 * @throws IOException */
	Map<String,Map<String,Integer>> aggregateMentionsByMMThisYear(final byte[][] auths, final Map<Long,List<String>> args) throws IOException;
	
	/** Method to compute the aggregation of the number of mention by mentioner and mentioned in the specified time range: Map<mentioned,Map<mentioner,times>> 
	 * @param auths the IDs of the users to filter the cells (if you only want the mentiones from these users)
	 * @param startTime the first millisecond to consider
	 * @param endTime the last millisecond to consider
	 * @params args the params in a map containing the id of the mentioned user and a list of the first and last row key to scan
	 * @return a Map<mentioned,Map<mentioner,times>> object
	 * @throws IOException */
	Map<String,Map<String,Integer>> aggregateMentionsByMMBackwards(final byte[][] auths, final long startTime, final long endTime, Map<Long,List<String>> args) throws IOException;
	
	/** Method to compute the aggregation of the number of mention by mentioned in the specified time range: Map<mentioned,times>
	 * @param auths the IDs of the users to filter the cells (if you only want the mentiones from these users)
	 * @params args the params in a map containing the id of the mentioned user and a list of the first and last row key to scan
	 * @return a Map<mentioned,times> object
	 * @throws IOException */
	Map<String,Integer> aggregateMentions(final byte[][] auths, final Map<Long,List<String>> args) throws IOException;

	/** Method to compute the aggregation of the number of mention by mentioned in this year's time range: Map<mentioned,times>
	 *  * @param auths the IDs of the users to filter the cells (if you only want the mentiones from these users)
	 * @params args the params in a map containing the id of the mentioned user and a list of the first and last row key to scan
	 * @return a Map<mentioned,times> object
	 * @throws IOException
	 */
	Map<String,Integer> aggregateMentionsThisYear(final byte[][] auths, final Map<Long,List<String>> args) throws IOException;
	
	/** Method to compute the aggregation of the number of mention by mentioned in the specified time range: Map<mentioned,times>
	 * @param auths the IDs of the users to filter the cells (if you only want the mentiones from these users)
	 * @param startTime the first millisecond to consider
	 * @param endTime the last millisecond to consider
	 * @params args the params in a map containing the id of the mentioned user and a list of the first and last row key to scan
	 * @return a Map<mentioned,times> object
	 * @throws IOException */
	Map<String,Integer> aggregateMentionsBackwards(final byte[][] auths, final long startTime, final long endTime, final Map<Long,List<String>> args) throws IOException;
}
