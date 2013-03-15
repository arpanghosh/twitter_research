package com.edge.twitter_research.collector_filter;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class CollectorDriver {

    private static Logger logger =
            Logger.getLogger(CollectorDriver.class);

    public static void main(String[] args){

        if (args.length < 2){
            System.out.println("Usage: CollectorDriver " +
                    "<collector_filter_root> <phrase1> <phrase2> ..... <phraseN>");
            System.exit(-1);
        }

        String filterTweetStoreLayoutFilePath = args[0] + "/" +
                Constants.FILTER_TWEET_STORE_TABLE_LAYOUT_FILE_NAME;
        String log4jPropertiesFilePath = args[0] + "/" +
                Constants.LOG4J_PROPERTIES_FILE_PATH;

        ArrayList<String> phrases = new ArrayList<String>();
        for (int i = 1; i < args.length; i++){
            phrases.add(args[i]);
        }

        PropertyConfigurator.configure(log4jPropertiesFilePath);

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Constants.OAUTH_CONSUMER_KEY)
                .setOAuthConsumerSecret(Constants.OAUTH_CONSUMER_SECRET)
                .setOAuthAccessToken(Constants.OAUTH_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Constants.OAUTH_ACCESS_TOKEN_SECRET);
        Configuration configuration = configurationBuilder.build();

        LinkedBlockingQueue<TweetPhraseMessage> tweetStorageQueue =
                new LinkedBlockingQueue<TweetPhraseMessage>();

        ArrayList<GetStatusesFilterStreamThread> getStatusesFilterStreamThreads
                = new ArrayList<GetStatusesFilterStreamThread>();

        for (String phrase : phrases){
            GetStatusesFilterStreamListener listener =
                    new GetStatusesFilterStreamListener(tweetStorageQueue,
                                                        log4jPropertiesFilePath,
                                                        phrase);
            TwitterStream twitterStream =
                    new TwitterStreamFactory(configuration).getInstance();
            twitterStream.addListener(listener);
            getStatusesFilterStreamThreads
                    .add(new GetStatusesFilterStreamThread(twitterStream, phrase));
        }

        Thread tweetStorageThread =
                new TweetStorageThread(tweetStorageQueue,
                                        filterTweetStoreLayoutFilePath,
                                        GlobalConstants.FILTER_TWEET_STORAGE_TABLE_NAME,
                                        log4jPropertiesFilePath);

        tweetStorageThread.start();
        for (GetStatusesFilterStreamThread getStatusesFilterStreamThread : getStatusesFilterStreamThreads ){
            getStatusesFilterStreamThread.start();
        }

        try{
            for (GetStatusesFilterStreamThread getStatusesFilterStreamThread : getStatusesFilterStreamThreads ){
                getStatusesFilterStreamThread.join();
            }
            tweetStorageThread.join();
        }catch (InterruptedException interruptedException){
            logger.warn("Exception while Collector threads are joining",
                        interruptedException);
        }
    }
}
