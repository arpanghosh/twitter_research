package com.edge.twitter_research.core;


public class GlobalConstants {

    /*Time Intervals*/
    public static final int BACKOFF_AFTER_TWITTER_API_FAILURE = 900;
    public static final int RATE_LIMIT_WINDOW = 900;


    /*Email Stuff*/
    public static final String EMAIL_USERNAME = "tweet.collector.alerts@gmail.com";
    public static final String EMAIL_PASSWORD = "collector";
    public static final String REPORTING_EMAIL_USERNAME = "arpanghosh@gmail.com";
    public static final String MAIL_SMTP_AUTH_PROPERTY = "mail.smtp.auth";
    public static final String MAIL_SMTP_AUTH_VALUE = "true";
    public static final String MAIL_SMTP_STARTTLS_ENABLE_PROPERTY = "mail.smtp.starttls.enable";
    public static final String MAIL_SMTP_STARTTLS_ENABLE_VALUE = "true";
    public static final String MAIL_SMTP_HOST_PROPERTY = "mail.smtp.host";
    public static final String MAIL_SMTP_HOST_VALUE = "smtp.gmail.com";
    public static final String MAIL_SMTP_PORT_PROPERTY = "mail.smtp.port";
    public static final String MAIL_SMTP_PORT_VALUE = "587";


    /*Kiji table names*/
    public static final String CATEGORY_TWEET_STORAGE_TABLE_NAME = "category_tweet_store";
    public static final String USER_LAST_TWEET_ID_TABLE_NAME = "users_last_tweet_id";
    public static final String SAMPLE_TWEET_STORAGE_TABLE_NAME = "sample_tweet_store";


    /*Columns in Kiji tweet tables*/
    public static final String TWEET_COLUMN_FAMILY_NAME = "tweet_object";
    public static final String TWEET_COLUMN_NAME = "tweet";
    public static final String LABEL_COLUMN_NAME = "relevance_label";


    /*Tweet properties*/
    public static final long INVALID_TWEET_ID = -1L;

}
