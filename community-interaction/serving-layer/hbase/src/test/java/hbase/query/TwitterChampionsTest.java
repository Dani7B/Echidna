package hbase.query;

import java.io.IOException;

import hbase.impls.HQueryManager;
import hbase.query.time.LastMonth;
import hbase.query.time.LastWeek;
import hbase.query.time.LastYear;
import hbase.query.time.MonthsAgo;
import hbase.query.time.WeeksAgo;

/**
 * @author Daniele Morgantini
 */
public class TwitterChampionsTest {

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

    	String info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the last month";
        printResult(queryManager, query, info);
        System.out.println("");
        
       
        
        final HQuery query5 = new HQuery()
								.users()
								.thatMentioned(new MonthsAgo(2), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.rankedById(true)
								.take(4);

        info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the last 2 months";
        printResult(queryManager, query5, info);
        System.out.println("");
        
		
		LastWeek lw = new LastWeek();
		final HQuery query7 = new HQuery()
							  .users()
							  .thatMentioned(lw, new AtLeast(1), new AtLeastTimes(1),
									  new Mention(14), new Mention(11), new Mention(12))
							  .rankedByHits(true)
							  .take(5);

		info = "The top 5 users that mentioned at least one among 11, 14 and 12 during last week [" +
				lw.getStart() + " - " + lw.getEnd() + "] \n";
		printResult(queryManager, query7, info);
		System.out.println("");
		
		final HQuery query8 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new WeeksAgo(5), new AtLeast(1),
		  					  new Mention(14), new Mention(11))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users that mentioned at least one among 11 and 14 in the last 5 weeks \n";
		printResult(queryManager, query8, info);
		System.out.println("");
        
		final HQuery query9 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new MonthsAgo(2), new AtLeast(1),
		  					  new Mention(14), new Mention(11), new Mention(12))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users by rank that mentioned at least one among 11, 14 and 12 in the last 2 months \n";
		printResult(queryManager, query9, info);
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
