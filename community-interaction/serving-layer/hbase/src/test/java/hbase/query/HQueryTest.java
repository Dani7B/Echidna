package hbase.query;

import java.io.IOException;

import hbase.impls.HQueryManager;
import hbase.query.time.LastMonth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Daniele Morgantini
 */
public class HQueryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HQueryTest.class);

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

        printResult(queryManager, query);
        
        
        final HQuery reversedQuery = new HQuery()
										.users()
										.whoFollow(new AtLeast(1), new Author(22), new Author(21), new Author(25))
										.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
										.rankedById(false)
										.take(4);
        
        printResult(queryManager, reversedQuery);

        
        final HQuery query2 = new HQuery()
								.users()
								.whoseFollowersFollow(new Author(21),new Author(22))
								.rankedByHits(true)
								.take(5);
        
        printResult(queryManager, query2);
        
        
        final HQuery query3 = new HQuery()
								.users()
								.whoseFollowersAreFollowedBy(new Author(3),new Author(5))
								.rankedByHits(true)
								.take(5);
		
        printResult(queryManager, query3);

        
        final HQuery complexQuery = new HQuery()
								.users()
								.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.whoFollow(new AtLeast(1), new Author(22), new Author(21), new Author(25))
								.whoseFollowersFollow(new Author(32),new Author(22))
								.rankedByHits(true)
								.take(5);

        printResult(queryManager, complexQuery);
        
        /*final HQuery query2 = new HQuery()
								.users()
								.whoseFollowers().follow(new Author(25))
								.rankedByHits(true)
								.take(10);*/
        
    }

    
    private static void printResult(HQueryManager queryManager, HQuery query) {
    	
    	Authors answer = null;
    	long start = System.currentTimeMillis();
    	try {
    		answer = queryManager.answer(query);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	long end = System.currentTimeMillis();
    	System.out.println(answer.toString() + " computed in: " + (end - start)+ " msec.");
            
    }
}
