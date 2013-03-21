package com.edge.twitter_research.collector_filter;


import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class CollectorDriver {

    private static Logger logger =
            Logger.getLogger(CollectorDriver.class);
    private static CrisisMailer crisisMailer =
            CrisisMailer.getCrisisMailer();

    public static void putToSleep(int seconds){
        boolean slept = false;
        do{
            try{
                Thread.sleep(seconds * 1000);
                slept = true;
            }catch (InterruptedException interruptedException){
                logger.warn("Exception while trying to sleep",
                        interruptedException);
            }
        }while(!slept);
    }

    public static void main(String[] args){

        if (args.length < 1){
            System.out.println("Usage: CollectorDriver " +
                    "<collector_filter_root>");
            System.exit(-1);
        }

        String filterTweetStoreLayoutFilePath = args[0] + "/" +
                Constants.FILTER_TWEET_STORE_TABLE_LAYOUT_FILE_NAME;
        String log4jPropertiesFilePath = args[0] + "/" +
                Constants.LOG4J_PROPERTIES_FILE_PATH;
        String phraseFilePath = args[0] + "/" +
                Constants.PHRASE_FILE_NAME;

        PropertyConfigurator.configure(log4jPropertiesFilePath);

        try{

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Constants.OAUTH_CONSUMER_KEY)
                .setOAuthConsumerSecret(Constants.OAUTH_CONSUMER_SECRET)
                .setOAuthAccessToken(Constants.OAUTH_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Constants.OAUTH_ACCESS_TOKEN_SECRET);
        Configuration configuration = configurationBuilder.build();

        LinkedBlockingQueue<TweetPhraseMessage> tweetStorageQueue =
                new LinkedBlockingQueue<TweetPhraseMessage>();


        Thread tweetStorageThread =
                new TweetStorageThread(tweetStorageQueue,
                                        filterTweetStoreLayoutFilePath,
                                        GlobalConstants.FILTER_TWEET_STORAGE_TABLE_NAME,
                                        log4jPropertiesFilePath);

        Thread phraseFetchingThread =
                new PhraseFetchingThread(log4jPropertiesFilePath,
                                        phraseFilePath,
                                        tweetStorageQueue,
                                        configuration);

        tweetStorageThread.start();
        phraseFetchingThread.start();


        tweetStorageThread.join();
        phraseFetchingThread.join();


        }catch (InterruptedException interruptedException){
            logger.warn("Exception while Collector threads are joining",
                        interruptedException);
        }catch (Exception unknownException){
            logger.error("Unknown Exception while collector_filter is starting",
                    unknownException);
            crisisMailer.sendEmailAlert(unknownException);
        }

        logger.error("CollectorDriver has stopped of own free will");
        crisisMailer.sendEmailAlert("collector_filter: CollectorDriver has stopped of own free will");
    }
}
