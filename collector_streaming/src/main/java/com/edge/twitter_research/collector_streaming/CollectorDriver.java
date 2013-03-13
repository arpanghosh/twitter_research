package com.edge.twitter_research.collector_streaming;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.Status;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class CollectorDriver {

    private static Logger logger =
            Logger.getLogger(CollectorDriver.class);

    public static void main(String[] args){

        if (args.length < 1){
            System.out.println("Usage: CollectorDriver " +
                    "<collector_streaming_root>");
            System.exit(-1);
        }

        String sampleTweetStoreLayoutFilePath = args[0] + "/" +
                Constants.SAMPLE_TWEET_STORE_TABLE_LAYOUT_FILE_NAME;
        String log4jPropertiesFilePath = args[0] + "/" +
                Constants.LOG4J_PROPERTIES_FILE_PATH;

        PropertyConfigurator.configure(log4jPropertiesFilePath);

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Constants.OAUTH_CONSUMER_KEY)
                .setOAuthConsumerSecret(Constants.OAUTH_CONSUMER_SECRET)
                .setOAuthAccessToken(Constants.OAUTH_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Constants.OAUTH_ACCESS_TOKEN_SECRET);

        LinkedBlockingQueue<Status> tweetStorageQueue =
                new LinkedBlockingQueue<Status>();

        GetStatusesSampleStreamListener listener =
                new GetStatusesSampleStreamListener(tweetStorageQueue,
                                                    log4jPropertiesFilePath);
        TwitterStream twitterStream =
                new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        twitterStream.addListener(listener);

        Thread getStatusesSampleStreamThread =
                new GetStatusesSampleStreamThread(twitterStream);

        Thread tweetStorageThread =
                new TweetStorageThread(tweetStorageQueue,
                                        sampleTweetStoreLayoutFilePath,
                                        GlobalConstants.SAMPLE_TWEET_STORAGE_TABLE_NAME,
                                        log4jPropertiesFilePath);


        //userFetchingQueue.add(new UserCategoryMessage(111757158L, "nascar"));

        tweetStorageThread.start();
        getStatusesSampleStreamThread.start();

        try{
            getStatusesSampleStreamThread.join();
            tweetStorageThread.join();
        }catch (InterruptedException interruptedException){
            logger.warn("Exception while Collector threads are joining",
                        interruptedException);
        }
    }
}
