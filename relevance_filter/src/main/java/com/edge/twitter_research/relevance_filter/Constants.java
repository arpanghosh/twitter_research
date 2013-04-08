package com.edge.twitter_research.relevance_filter;


public class Constants {

    public static final String LOG4J_PROPERTIES_FILE_PATH =
            "src/main/java/resources/log4j.properties";

    public static final String TWEET_COLUMN_FAMILY_NAME = "tweet_object";
    public static final String TWEET_OBJECT_COLUMN_NAME = "tweet";
    public static final String TWEET_RELEVANCE_LABEL_COLUMN_NAME = "relevance_label";

    public static final String RELEVANT_RELEVANCE_LABEL = "topic-related";
    public static final String NOT_RELEVANT_RELEVANCE_LABEL = "not-topic-related";
    public static final String NOT_ENGLISH_RELEVANCE_LABEL = "not-english";


}
