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

    	final HQuery query = new HQuery()
								.users()
								.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(11),
											new Mention(14), new Mention(12))
								.whoFollow(new AtLeast(2), new Author(22), new Author(21), new Author(25))
								.rankedById(true)
								.take(2);

        final HQueryManager queryManager = new HQueryManager();
        Authors answer = null;
        try {
			answer = queryManager.answer(query);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        System.out.println(answer.toString());
    }

}
