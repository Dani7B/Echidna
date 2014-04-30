package org.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class AuthorAggregatorEndpoint extends BaseEndpointCoprocessor implements
	AuthorAggregatorProtocol {

	@Override
	public Map<String,Map<String,Integer>> aggregateMentionsByMM(byte[][] auths, Map<Long,List<String>> args) throws IOException {
		
		Map<String,Map<String,Integer>> general = new HashMap<String,Map<String,Integer>>();
		RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();
		
		for(Map.Entry<Long, List<String>> m : args.entrySet()){
			Map<String,Integer> map = new HashMap<String,Integer>();
			
			String firstRow = m.getValue().get(0);
			String lastRow = m.getValue().get(1);
			Scan scan;
			
			byte[] lowerRow = Bytes.toBytes(firstRow);
			byte[] upperRow = Bytes.toBytes(lastRow);
			if(Bytes.equals(lowerRow,upperRow)) {
				Get get = new Get(lowerRow);
				scan = new Scan(get);
			}
			else {
				scan = new Scan(lowerRow,upperRow);
			}
			if(auths.length > 0) {
				FilterList qualifierFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
				for(byte[] q : auths) {
					qualifierFilters.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
							new BinaryComparator(q)));
				}
			scan.setFilter(qualifierFilters);
			}									
			
			InternalScanner scanner = environment.getRegion().getScanner(scan);
			try {
				List<KeyValue> curVals = new ArrayList<KeyValue>();
			    boolean done = false;
			    do {
			    	curVals.clear();
			        done = scanner.next(curVals);
			        for(KeyValue kv : curVals) {
			        	int value = Bytes.toInt(kv.getValue());
						String mentioner = Bytes.toString(kv.getQualifier());
						if(map.containsKey(mentioner)) {
							value += map.get(mentioner);
						}
						map.put(mentioner, value);
			        }
			    } while (done);
		    } finally {
		      scanner.close();
		    }
			general.put(String.valueOf(m.getKey()), map);
		}
		return general;
	}

	@Override
	public Map<String,Map<String,Integer>> aggregateMentionsByMMThisYear(byte[][] auths, Map<Long,List<String>> args) throws IOException {
		
		Map<String,Map<String,Integer>> general = new HashMap<String,Map<String,Integer>>();
		RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();
		
		for(Map.Entry<Long, List<String>> m : args.entrySet()){
			Map<String,Integer> map = new HashMap<String,Integer>();
			
			String firstRow = m.getValue().get(0);
			String lastRow = m.getValue().get(1);
				
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			
			Filter rPref = new PrefixFilter(Bytes.toBytes(firstRow));
			fList.addFilter(rPref);
			
			if(auths.length > 0) {
				FilterList qualifierFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
				for(byte[] prefix : auths) {
					Filter prefixFilter = new ColumnPrefixFilter(prefix);
					qualifierFilters.addFilter(prefixFilter);
				}
				fList.addFilter(qualifierFilters);
			}
			
			Scan scan = new Scan()
						.setFilter(fList);				
			
			InternalScanner scanner = environment.getRegion().getScanner(scan);
			try {
				List<KeyValue> curVals = new ArrayList<KeyValue>();
			    boolean done = false;
			    do {
			    	curVals.clear();
			        done = scanner.next(curVals);
			        for(KeyValue kv : curVals) {
			        	int value = Bytes.toInt(kv.getValue());
						String mentioner = Bytes.toString(kv.getQualifier());
						if(map.containsKey(mentioner)) {
							value += map.get(mentioner);
						}
						map.put(mentioner, value);
			        }
			    } while (done);
		    } finally {
		      scanner.close();
		    }
			general.put(String.valueOf(m.getKey()), map);
		}
		return general;
	}
}
