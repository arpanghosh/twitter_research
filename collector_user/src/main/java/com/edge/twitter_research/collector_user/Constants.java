package com.edge.twitter_research.collector_user;


public class Constants {

    /*Twitter authentication stuff*/
    public static final String OAUTH_CONSUMER_KEY =
            "xyAbNH6o0VOAjauxxMeg";
    public static final String OAUTH_CONSUMER_SECRET =
            "eW8zggbPixtmfdPSuT03XeBj3ZBNTbAW3VmhNmG084";
    public static final String OAUTH_ACCESS_TOKEN =
            "377194808-IV7W0NxAsZGprmoLSelWRgJd8Cz9KvDsSBzBNVOl";
    public static final String OAUTH_ACCESS_TOKEN_SECRET =
            "iNMilIHwv0ja067SUqHBHb8CbwnnaeesIGBHyZwFE";


    /*File paths*/
    public static final String USER_TWEET_STORE_TABLE_LAYOUT_FILE_NAME =
            "/user_tweet_store_layout.json";
    public static final String LOG4J_PROPERTIES_FILE_PATH =
            System.getProperty("user.home") + "/twitter_research/collector_user/log4j.properties";



    public static final int USER_FILE_CHECKING_INTERVAL = 3600;
    public static final int MAX_TWEETS_IN_ONE_REQUEST = 200;

}
