package com.edge.twitter_research.collector_filter;

import org.apache.log4j.PropertyConfigurator;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiTable;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import  com.edge.twitter_research.core.*;
import twitter4j.Status;

public class TweetStorageThread extends Thread {

    private LinkedBlockingQueue<Status> inputQueue;
    private KijiConnection kijiConnection;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(TweetStorageThread.class);
    private long tweetCounter = 0L;

    public TweetStorageThread(LinkedBlockingQueue<Status> inputQueue,
                              String tableName){
        this.inputQueue = inputQueue;
        this.kijiConnection
                = new KijiConnection(this.getClass()
                                    .getResourceAsStream(Constants
                                            .FILTER_TWEET_STORE_TABLE_LAYOUT_FILE_NAME), tableName);
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);
    }

    public void run(){
        Status tweet;
        while(true){
            try{
                tweet = inputQueue.take();
                storeTweet(tweet);
                tweetCounter++;

                if (tweetCounter % 1000 == 0){
                    logger.warn(DateTimeCreator.getDateTimeString() + " - " +
                            "Total Tweets so far : " + tweetCounter);
                }

            }catch (InterruptedException interruptedException){
                logger.warn("Exception while 'taking' item from queue",
                        interruptedException);
            }catch (IOException ioException){
                logger.error("IOException in TweetStorageThread",
                        ioException);
                crisisMailer.sendEmailAlert(ioException);
            }catch (Exception unknownException){
                logger.error("Unknown Exception in TweetStorageThread",
                        unknownException);
                crisisMailer.sendEmailAlert(unknownException);
            }
        }
    }


    private void storeTweet(Status tweet)
        throws IOException{
        if (kijiConnection.isValidKijiConnection()){
                EntityId tweetId =
                        kijiConnection.kijiTable.getEntityId(tweet.getId());
                kijiConnection.kijiTableWriter.put(tweetId,
                        GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.TWEET_COLUMN_NAME,
                        System.currentTimeMillis(),
                        SimpleTweetGenerator.generateSimpleTweet(tweet));
                kijiConnection.kijiTableWriter.put(tweetId,
                        GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        System.currentTimeMillis(),
                        null);
                /*
                kijiConnection.kijiTableWriter.put(tweetId,
                        GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.COMPANY_DATA_COLUMN_NAME,
                        System.currentTimeMillis(),
                        null);
                */
        }
    }
}
