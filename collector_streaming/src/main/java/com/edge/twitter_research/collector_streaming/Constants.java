package com.edge.twitter_research.collector_streaming;


public class Constants {

    /*Twitter authentication stuff*/
    public static final String OAUTH_CONSUMER_KEY =
            "6Tf4wzQpPjOFuUQLI6lqZA";
    public static final String OAUTH_CONSUMER_SECRET =
            "Z41roc5WHARx9kvT1q7o6nLIsRBxB2VOaAwgHEvCM";
    public static final String OAUTH_ACCESS_TOKEN =
            "377194808-mr5L80fWWgqpb4imqXnOX1Mvx5S1Fa7k8v8Ovvxc";
    public static final String OAUTH_ACCESS_TOKEN_SECRET =
            "KdmSpkKY2YJIcQRr6ph4VqksCEBKAFdqj27k1c2KY";


    /*File paths*/
    public static final String SAMPLE_TWEET_STORE_TABLE_LAYOUT_FILE_NAME =
            "/sample_tweet_store_layout.json";
    public static final String LOG4J_PROPERTIES_FILE_PATH =
            System.getProperty("user.home") + "/twitter_research/collector_streaming/log4j.properties";
}
