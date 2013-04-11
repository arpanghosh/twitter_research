package com.edge.twitter_research.collector_streaming;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.Status;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class StreamingCollectorDriver {

    private static Logger logger =
            Logger.getLogger(StreamingCollectorDriver.class);
    private static CrisisMailer crisisMailer =
            CrisisMailer.getCrisisMailer();

    public static void main(String[] args){

        PropertyConfigurator.configure(StreamingCollectorDriver.class.getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));

        try{

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
                                        GlobalConstants.SAMPLE_TWEET_STORAGE_TABLE_NAME);

        tweetStorageThread.start();
        getStatusesSampleStreamThread.start();


        getStatusesSampleStreamThread.join();
        tweetStorageThread.join();


        }catch (InterruptedException interruptedException){
            logger.warn("Exception while Collector threads are joining",
                        interruptedException);
        }catch (Exception unknownException){
            logger.error("Unknown Exception in StreamingCollectorDriver",
                    unknownException);
            crisisMailer.sendEmailAlert(unknownException);
        }

        logger.error("StreamingCollectorDriver has stopped of own free will");
        crisisMailer.sendEmailAlert("collector_streaming: StreamingCollectorDriver has stopped of own free will");
    }
}
