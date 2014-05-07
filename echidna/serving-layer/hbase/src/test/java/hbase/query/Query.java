package hbase.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matteo Moci ( matteo (dot) moci (at) gmail (dot) com )
 */
public class Query {

    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    public void main(String[] args) {

        postsQuery();
        usersQuery();
    }

    private void postsQuery() {

        final Query query = new Query()
                .posts()
                .thatMentioned(new LastWeek(), new AtLeast(1), new Mention("@nike"))
                .thatAreAbout(new Topic("a"), new Topic("b"))
                .writtenBy(new Blogger())
                .rankedBy(new Engagement())
                .take(10);

        final Object hbaseClient = null;
        final QueryExecutor queryExecutor = new QueryExecutor(hbaseClient);
        final QueryExecutor.Result answer = queryExecutor.answer(query);
    }

    private void usersQuery() {

        final Query query = new Query().users()
                .thatMentioned(new LastWeek(), new AtLeast(1), new Mention("@nike"))
                .whoAre(new Female().under(40))
                .whoFollow(new User("@ramsay"), new User("@nando"))
                .whoAreLinkedTo(new User("@mulinobianco"))
                .in(new LessThan(new Hops(4)))
                .locatedIn(new City("London"))
                .rankedByInfluence()
                .take(10);

        final Object hbaseClient = null;
        final QueryExecutor queryExecutor = new QueryExecutor(hbaseClient);
        final QueryExecutor.Result answer = queryExecutor.answer(query);
    }

    private Authors posts() {

        return null;
    }

    private Authors users() {

        return null;
    }

    private class LastWeek {

    }

    private class AtLeast {

        public AtLeast(int i) {

        }
    }

    private class Mention {

        public Mention(String s) {

        }
    }

    private class Authors {

        public Authors thatMentioned(LastWeek lastWeek, AtLeast atLeast, Mention mention) {

            return null;
        }

        public Authors whoAre(DemographicFilter demographicFilter) {

            return null;
        }

        public Query take(int amount) {

            return null;
        }

        public Authors whoFollow(User... user) {

            return null;
        }

        public Links whoAreLinkedTo(User user) {

            return null;
        }

        public Authors rankedByInfluence() {

            return null;
        }

        public Authors writtenBy(DemographicFilter under) {

            return null;
        }

        public Authors rankedBy(Engagement engagement) {

            return null;
        }

        public Authors thatAreAbout(Topic a, Topic b) {

            return null;
        }

        public Authors locatedIn(City london) {

            return null;
        }

        private class Links {

            public Authors in(Ranges limit) {

                return null;
            }
        }
    }

    private class Female extends DemographicFilter {

        public Female under(int i) {

            return null;
        }
    }

    private class Blogger extends DemographicFilter {

        private Blogger() {

        }
    }

    private abstract class DemographicFilter {

    }

    private class User {

        public User(String s) {

        }
    }

    private class Hops {

        public Hops(int i) {

        }
    }

    private class LessThan extends Ranges {

        public LessThan(Hops hops) {

        }
    }

    private abstract class Ranges {

    }

    private class QueryExecutor {

        public QueryExecutor(Object hbaseClient) {

        }

        public Result answer(Query query) {

            return null;
        }

        private class Result {

        }
    }

    private class Engagement {

    }

    private class Topic {

        public Topic(String a) {

        }
    }

    private class City {

        public City(String london) {

        }
    }
}
