package com.edge.twitter_research.collector_filter;


import twitter4j.Status;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class FilterCollectorDriver {

    private static Logger logger =
            Logger.getLogger(FilterCollectorDriver.class);
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
            System.out.println("Usage: FilterCollectorDriver " +
                    "<phrase_file_path>");
            System.exit(-1);
        }

        String phraseFilePath = args[0];

        PropertyConfigurator.configure(FilterCollectorDriver.class.getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));

        try{

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Constants.OAUTH_CONSUMER_KEY)
                .setOAuthConsumerSecret(Constants.OAUTH_CONSUMER_SECRET)
                .setOAuthAccessToken(Constants.OAUTH_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Constants.OAUTH_ACCESS_TOKEN_SECRET);
        Configuration configuration = configurationBuilder.build();

        LinkedBlockingQueue<Status> tweetStorageQueue =
                new LinkedBlockingQueue<Status>();


        Thread tweetStorageThread =
                new TweetStorageThread(tweetStorageQueue,
                                        GlobalConstants.FILTER_TWEET_STORAGE_TABLE_NAME);

        Thread phraseFetchingThread =
                new PhraseFetchingThread(phraseFilePath,
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

        logger.error("FilterCollectorDriver has stopped of own free will");
        crisisMailer.sendEmailAlert("collector_filter: FilterCollectorDriver has stopped of own free will");
    }
}
