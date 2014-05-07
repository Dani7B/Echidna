package hbase.query;

import java.io.IOException;

import hbase.impls.HQueryManager;
import hbase.query.time.LastMonth;
import hbase.query.time.LastWeek;
import hbase.query.time.LastYear;
import hbase.query.time.MonthsAgo;
import hbase.query.time.ThisYear;
import hbase.query.time.WeeksAgo;

/**
 * Test to query against Twitter Champions repository
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
								.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(34613288),
											new Mention(22910295), new Mention(253508662))
								.rankedById(false)
								.take(4);

    	String info = "The top 4 users by id that mentioned at least 1 among \n Arsenal(34613288), Chelsea(22910295) and Juventus(253508662) in the last month \n";
        printResult(queryManager, query, info);
        System.out.println("");
        
       
        
        final HQuery query1 = new HQuery()
								.users()
								.thatMentioned(new MonthsAgo(4), new AtLeast(1), new Mention(34613288),
											new Mention(22910295), new Mention(253508662))
								.rankedById(true)
								.take(4);

        info = "The top 4 users by id that mentioned at least 1 among \n Arsenal(34613288), Chelsea(22910295) and Juventus(253508662) in the last 4 months \n";
        printResult(queryManager, query1, info);
        System.out.println("");
        
		
		LastWeek lw = new LastWeek();
		final HQuery query2 = new HQuery()
							  .users()
							  .thatMentioned(new LastWeek(), new AtLeast(1), new AtLeastTimes(1),
									  new Mention(34613288), new Mention(22910295), new Mention(253508662))
							  .rankedByHits(true)
							  .take(5);

		info = "The top 5 users that mentioned at least 1 among \n Arsenal(34613288), Chelsea(22910295) and Juventus(253508662) during last week [" +
				lw.getStart() + " - " + lw.getEnd() + "] \n";
		printResult(queryManager, query2, info);
		System.out.println("");
		
		final HQuery query3 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new WeeksAgo(5), new AtLeast(1),
		  					  new Mention(34613288), new Mention(22910295))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users that mentioned at least 1 among \n Arsenal(34613288) and Chelsea(22910295) in the last 5 weeks \n";
		printResult(queryManager, query3, info);
		System.out.println("");
        
		final HQuery query4 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new LastYear(), new AtLeast(2),
		  					  new Mention(34613288), new Mention(22910295), new Mention(253508662))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users by rank that mentioned at least 2 among \n Arsenal(34613288), Chelsea(22910295) and Juventus(253508662) in the last year \n";
		printResult(queryManager, query4, info);
		System.out.println("");
		
		final HQuery yearQuery = new HQuery()
								.users()
								.thatMentioned(new ThisYear(), new AtLeast(1), new Mention(34613288),
											new Mention(22910295), new Mention(253508662))
								.rankedByHits(true)
								.take(5);

		info = "The top 5 users (ranked by hits) that mentioned at least 1 among \n Arsenal(34613288), Chelsea(22910295) and Juventus(253508662) during this year \n";
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
