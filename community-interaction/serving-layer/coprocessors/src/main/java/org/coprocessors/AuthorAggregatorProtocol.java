package org.coprocessors;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface AuthorAggregatorProtocol extends CoprocessorProtocol {

	/** Method to compute the map of */
	Map<String,Map<String,Integer>> aggregateMentionsByMM(byte[][] auths, Map<Long,List<String>> args) throws IOException;

	Map<String, Map<String, Integer>> aggregateMentionsByMMThisYear(byte[][] auths, Map<Long,List<String>> args) throws IOException;
	
	//Map<String, Integer> getAuthorsThatMentionedFixedTime(byte[][] auths, AuthorsThatMentionedFixedTime atmft) throws IOException;
	
}
