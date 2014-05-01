package hbase.query;

import java.io.IOException;

import hbase.impls.HQueryManager;
import hbase.query.time.LastMonth;
import hbase.query.time.LastYear;
import hbase.query.time.MonthsAgo;
import hbase.query.time.WeeksAgo;

/**
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
								.rankedById(false)
								.take(4);

    	String info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the last month \n";
        printResult(queryManager, query, info);
        System.out.println("");
        
        
        
        
        final HQuery complexQuery = new HQuery()
								.users()
								.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedByHits(true)
								.take(5);

        info = "The top 5 users that mentioned at least 1 among 11, 14, 12 in the last month \n";
        printResult(queryManager, complexQuery, info);
        System.out.println("");

        
       

        
        final HQuery query5 = new HQuery()
								.users()
								.thatMentioned(new MonthsAgo(3), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedById(true)
								.take(4);

        info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the last 3 months \n";
        printResult(queryManager, query5, info);
        System.out.println("");
        
		
		final HQuery query8 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new WeeksAgo(10), new AtLeast(1),
		  					  new Mention(14), new Mention(11))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users that mentioned at least 1 among 11 and 14 in the last 10 weeks \n";
		printResult(queryManager, query8, info);
		System.out.println("");
        
		final HQuery query9 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new MonthsAgo(4), new AtLeast(1),
		  					  new Mention(14), new Mention(11), new Mention(12))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users by rank that mentioned at least 1 among 11, 14 and 12 in the last 4 months \n";
		printResult(queryManager, query9, info);
		System.out.println("");
		
		final HQuery query9Mentioned = new HQuery()
		  							   .users()
		  							   .mentioned(new MonthsAgo(4), new AtLeastTimes(3),
		  									   new Mention(4), new Mention(3), new Mention(2))
		  							   .rankedByHits(true)
		  							   .take(3);

		info = "The top 3 users (by rank) mentioned at least 3 times among 4, 3 and 2 in the last 4 months \n";
		printResult(queryManager, query9Mentioned, info);
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
		
		
		final HQuery yearQueryMentioned = new HQuery()
										 .users()
										 .mentioned(new LastYear(), new AtLeastTimes(3), new Mention(3),
												 new Mention(2), new Mention(4))
										 .rankedByHits(true)
										 .take(3);

		info = "The top 3 users (ranked by hits) mentioned at least 3 times among 3, 2, 4 in the last year \n";
		printResult(queryManager, yearQueryMentioned, info);
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
