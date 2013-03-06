package com.edge.twitter_research.collector_streaming;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.Status;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;


public class CollectorDriver {

    public static void main(String[] args){

        if (args.length < 1){
            System.out.println("Usage: CollectorDriver " +
                    "sample_tweet_store_layout file path>");
            return;
        }

        String sampleTweetStoreLayoutFilePath = args[0];


        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Constants.OAUTH_CONSUMER_KEY)
                .setOAuthConsumerSecret(Constants.OAUTH_CONSUMER_SECRET)
                .setOAuthAccessToken(Constants.OAUTH_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Constants.OAUTH_ACCESS_TOKEN_SECRET);

        LinkedBlockingQueue<Status> tweetStorageQueue =
                new LinkedBlockingQueue<Status>();

        GetStatusesSampleStreamListener listener =
                new GetStatusesSampleStreamListener(tweetStorageQueue);
        TwitterStream twitterStream =
                new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        twitterStream.addListener(listener);

        Thread getStatusesSampleStreamThread =
                new GetStatusesSampleStreamThread(twitterStream);

        Thread tweetStorageThread =
                new TweetStorageThread(tweetStorageQueue,
                                        sampleTweetStoreLayoutFilePath,
                                        GlobalConstants.SAMPLE_TWEET_STORAGE_TABLE_NAME);


        //userFetchingQueue.add(new UserCategoryMessage(111757158L, "nascar"));

        tweetStorageThread.start();
        getStatusesSampleStreamThread.start();

        try{
            getStatusesSampleStreamThread.join();
            tweetStorageThread.join();
        }catch (InterruptedException interruptedException){
            interruptedException.printStackTrace();
        }
    }
}
