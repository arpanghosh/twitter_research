package com.edge.twitter_research.collector_categories;


public class Constants {

    /*Time intervals*/
    public static final int COLLECTION_INTERVAL_IN_SECS = 3600;
    public static final int MAX_TWEETS_IN_ONE_REQUEST = 200;
    public static final int QUEUE_MEASUREMENT_INTERVAL_IN_SECS = 60;

    /*Twitter authentication stuff*/
    public static final String OAUTH_CONSUMER_KEY =
            "0Haydyg1GCJnR08o3HEJw";
    public static final String OAUTH_CONSUMER_SECRET =
            "wYIvPbxJBNPGzsaNP0H1ya9HbukoB8pTTVnEb6X0";
    public static final String OAUTH_ACCESS_TOKEN =
            "377194808-h9vmTsUeCpRG5YMdSFgkyki2dCEzMPbV8GfJZ1po";
    public static final String OAUTH_ACCESS_TOKEN_SECRET =
            "dLn5F5VFeahhl9Qx1FleZ30JKbzxmt7mbGSfW5NwiN8";

    /*File paths*/
    public static final String CATEGORY_TWEET_STORE_TABLE_LAYOUT_FILE_NAME =
            "/category_tweet_store_layout.json";
    public static final String USERS_LAST_TWEET_ID_STORE_TABLE_LAYOUT_FILE_NAME =
            "/users_last_tweet_id_layout.json";
    public static final String LOG4J_PROPERTIES_FILE_PATH =
            "/log4j.properties";

    /*Column details*/
    public static final String LAST_TWEET_ID_COLUMN_FAMILY_NAME = "last_since_id";
    public static final String LAST_TWEET_ID_COLUMN_NAME = "since_id";
}
