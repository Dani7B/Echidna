package hbase.query;

import java.io.IOException;

import hbase.impls.HQueryManager;
import hbase.query.time.LastMonth;
import hbase.query.time.LastYear;
import hbase.query.time.LastYearFromNow;
import hbase.query.time.MonthsAgo;
import hbase.query.time.WeeksAgo;

/**
 * Simple class to test the queries based on mentions, initially developed to test
 * the aggregations performed by coprocessors
 * @author Daniele Morgantini
 */
public class HQueryAggregatorTest {

    public static void main(String[] args) {

        usersQuery();
    }

    private static void usersQuery() {

        final HQueryManager queryManager = new HQueryManager();
    	
    	final HQuery query = new HQuery()
								.users()
								.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedById(true)
								.take(4);

    	String info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the last month \n";
        printResult(queryManager, query, info);
        System.out.println("");
        
        
        final HQuery query2 = new HQuery()
								.users()
								.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedByHits(true)
								.take(5);

        info = "The top 5 users that mentioned at least 1 among 11, 14, 12 in the last month, ranked by hits \n";
        printResult(queryManager, query2, info);
        System.out.println("");

        
        final HQuery query3 = new HQuery()
								.users()
								.thatMentioned(new MonthsAgo(3), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedById(true)
								.take(4);

        info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the last 3 months \n";
        printResult(queryManager, query3, info);
        System.out.println("");
        
		
		final HQuery query4 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new WeeksAgo(10), new AtLeast(1),
		  					  new Mention(14), new Mention(11))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users that mentioned at least 1 among 11 and 14 in the last 10 weeks, ranked by hits \n";
		printResult(queryManager, query4, info);
		System.out.println("");
        
		final HQuery query5 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new MonthsAgo(4), new AtLeast(1),
		  					  new Mention(14), new Mention(11), new Mention(12))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users by rank that mentioned at least 1 among 11, 14 and 12 in the last 4 months \n";
		printResult(queryManager, query5, info);
		System.out.println("");
		
		
		final HQuery yearQuery = new HQuery()
								.users()
								.thatMentioned(new LastYear(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedByHits(true)
								.take(5);

		info = "The top 5 users (ranked by hits) that mentioned at least 1 among 11, 14, 12 in the last year \n";
		printResult(queryManager, yearQuery, info);
		System.out.println("");
		
		
		final HQuery queryMentioned = new HQuery()
									   .users()
									   .mentioned(new MonthsAgo(4), new AtLeastTimes(3),
											   new Mention(14), new Mention(11), new Mention(12))
									   .rankedByHits(true)
									   .take(3);

		info = "The top 3 users (by rank) mentioned at least 3 times among 14, 11 and 12 in the last 4 months \n";
		printResult(queryManager, queryMentioned, info);
		System.out.println("");
		
		
		final HQuery yearQueryMentioned = new HQuery()
										 .users()
										 .mentioned(new LastYear(), new AtLeastTimes(3), new Mention(11),
												 new Mention(12), new Mention(14))
										 .rankedByHits(true)
										 .take(3);

		info = "The top 3 users (ranked by hits) mentioned at least 3 times among 11, 12, 14 in the last year \n";
		printResult(queryManager, yearQueryMentioned, info);
		System.out.println("");
		
		final HQuery query6 = new HQuery()
								.users()
								.thatMentioned(new LastYearFromNow(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedById(true)
								.take(4);

		info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the very last year from now \n";
		printResult(queryManager, query6, info);
		System.out.println("");
		
		
		final HQuery query7 = new HQuery()
								.users()
								.mentioned(new LastYearFromNow(), new AtLeastTimes(3), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedByHits(true)
								.take(3);

		info = "The top 3 users ranked by hits mentioned at least 3 times among 11, 14, 12 in the very last year from now \n";
		printResult(queryManager, query7, info);
		System.out.println("");
    }

    
    private static void printResult(HQueryManager queryManager, HQuery query, String header) {
    	
    	Authors answer = null;
    	long start = System.currentTimeMillis();
    	try {
    		answer = queryManager.answer(query);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	long end = System.currentTimeMillis();
    	System.out.println(header + " " + answer.toString() 
    			+ " computed in: " + (end - start)+ " msec.");
            
    }
}
