package hbase.query;

import java.io.IOException;

import hbase.impls.HQueryManager;
import hbase.query.time.LastMonth;
import hbase.query.time.LastWeek;
import hbase.query.time.MonthsAgo;
import hbase.query.time.WeeksAgo;

/**
 * @author Daniele Morgantini
 */
public class HQueryTest {

    public static void main(String[] args) {

        usersQuery();
    }

    private static void usersQuery() {

        final HQueryManager queryManager = new HQueryManager();
    	
    	final HQuery query = new HQuery()
								.users()
								.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.whoFollow(new AtLeast(1), new Author(22), new Author(21), new Author(25))
								.rankedById(false)
								.take(4);

    	String info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the last month \n"
    			+ " and follow at least 1 among 22, 21, 25 \n";
        printResult(queryManager, query, info);
        System.out.println("");
        
        
        final HQuery reversedQuery = new HQuery()
										.users()
										.whoFollow(new AtLeast(1), new Author(22), new Author(21), new Author(25))
										.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
										.rankedById(false)
										.take(4);
        
    	info = "The top 4 users by id that follow at least 1 among 22, 21, 25 and \n"
    			+ " mentioned at least 1 among 11, 14, 12 in the last month \n";
        printResult(queryManager, reversedQuery, info);
        System.out.println("");

        
        final HQuery query2 = new HQuery()
								.users()
								.whoseFollowersFollow(new Author(21),new Author(22))
								.rankedByHits(true)
								.take(5);
        
        info = "The top 5 users whose followers follow 21 or 22 \n";
        printResult(queryManager, query2, info);
        System.out.println("");

        
        final HQuery query3 = new HQuery()
								.users()
								.whoseFollowersAreFollowedBy(new Author(3),new Author(5))
								.rankedByHits(true)
								.take(5);
		
        info = "The top 5 users whose followers are followed by 3 or 5 \n";
        printResult(queryManager, query3, info);
        System.out.println("");

        
        final HQuery complexQuery = new HQuery()
								.users()
								.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.whoFollow(new AtLeast(1), new Author(22), new Author(21), new Author(25))
								.whoseFollowersFollow(new Author(32),new Author(22))
								.rankedByHits(true)
								.take(5);

        info = "The top 5 users that mentioned at least 1 among 11, 14, 12 in the last month, \n"
    			+ "follow at least 1 among 22, 21, 25 and \n"
    			+ "whose followers follow 32 or 22 \n";
        printResult(queryManager, complexQuery, info);
        System.out.println("");

        
        final HQuery query4 = new HQuery()
								.users()
								.whoseFollowersMentioned(new LastMonth(), new AtLeast(1), new AtLeastTimes(1),
										new Mention(11), new Mention(14), new Mention(12))
								.rankedByHits(true)
								.take(5);

        info = "The top 5 users whose followers mentioned one or more times \n"
        		+ " at least one among 11, 14 and 12 in the last month \n";
        printResult(queryManager, query4, info);
        System.out.println("");

        
        final HQuery query5 = new HQuery()
								.users()
								.thatMentioned(new MonthsAgo(2), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.whoFollow(new AtLeast(1), new Author(22), new Author(21), new Author(25))
								.rankedById(true)
								.take(4);

        info = "The top 4 users by id that mentioned at least 1 among 11, 14, 12 in the last 2 months \n"
        		+ " and follow at least 1 among 22, 21, 25 \n";
        printResult(queryManager, query5, info);
        System.out.println("");
        
        final HQuery query6 = new HQuery()
								.users()
								.whoseFollowersMentioned(new MonthsAgo(2), new AtLeast(1), new AtLeastTimes(2),
										new Mention(14), new Mention(11), new Mention(12))
								.rankedByHits(true)
								.take(5);

		info = "The top 5 users whose followers mentioned two or more times \n"
		+ " at least one among 11, 14 and 12 in the last 2 months \n";
		printResult(queryManager, query6, info);
		System.out.println("");
		
		
		final HQuery query7 = new HQuery()
							  .users()
							  .thatMentioned(new LastWeek(), new AtLeast(1),
									  new Mention(14), new Mention(11), new Mention(12))
							  .rankedByHits(true)
							  .take(5);

		info = "The top 5 users who mentioned at least one among 11, 14 and 12 during last week \n";
		printResult(queryManager, query7, info);
		System.out.println("");
		
		final HQuery query8 = new HQuery()
		  					  .users()
		  					  .thatMentioned(new WeeksAgo(5), new AtLeast(1),
		  					  new Mention(14), new Mention(11))
		  					  .rankedByHits(true)
		  					  .take(5);

		info = "The top 5 users who mentioned at least one among 11 and 14 in the last 5 weeks \n";
		printResult(queryManager, query8, info);
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
