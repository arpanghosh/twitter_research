package com.edge.twitter_research.core;


public class Constants {

    /* Twitter endpoints*/
    public static final String USERS_SUGGESTIONS_ENDPOINT = "/users/suggestions";
    public static final String USERS_SUGGESTIONS_SLUG_ENDPOINT = "/users/suggestions/:slug";
    public static final String STATUSES_USER_TIMELINE_ENDPOINT = "/statuses/user_timeline";


    /* Keys for retrieving rate limit info*/
    public static final String USERS_RESOURCE = "users";
    public static final String STATUSES_RESOURCE = "statuses";


    /*Time intervals*/
    public static final int COLLECTION_INTERVAL_IN_SECS = 3600;
    public static final int BACKOFF_AFTER_TWITTER_API_FAILURE = 900;
    public static final int MAX_TWEETS_IN_ONE_REQUEST = 200;
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
    public static final String TWEET_STORAGE_TABLE_NAME = "category_tweet_store";
    public static final String USER_LAST_TWEET_ID_TABLE_NAME = "users_last_tweet_id";


    /*Twitter authentication stuff*/
    public static final String OAUTH_CONSUMER_KEY =
            "0Haydyg1GCJnR08o3HEJw";
    public static final String OAUTH_CONSUMER_SECRET =
            "wYIvPbxJBNPGzsaNP0H1ya9HbukoB8pTTVnEb6X0";
    public static final String OAUTH_ACCESS_TOKEN =
            "377194808-h9vmTsUeCpRG5YMdSFgkyki2dCEzMPbV8GfJZ1po";
    public static final String OAUTH_ACCESS_TOKEN_SECRET =
            "dLn5F5VFeahhl9Qx1FleZ30JKbzxmt7mbGSfW5NwiN8";

}
