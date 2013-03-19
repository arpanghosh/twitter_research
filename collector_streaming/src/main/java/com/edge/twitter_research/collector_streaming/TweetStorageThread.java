package com.edge.twitter_research.collector_streaming;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiTable;

import twitter4j.*;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import  com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TweetStorageThread extends Thread {

    private LinkedBlockingQueue<Status> inputQueue;
    private KijiConnection kijiConnection;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(TweetStorageThread.class);
    private long tweetCounter = 0L;

    public TweetStorageThread(LinkedBlockingQueue<Status> inputQueue,
                              String tableLayoutPath,
                              String tableName,
                              String log4jPropertiesFilePath){
        this.inputQueue = inputQueue;
        this.kijiConnection = new KijiConnection(tableLayoutPath, tableName);
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(log4jPropertiesFilePath);
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
                logger.warn("Exception while taking an item from the queue",
                        interruptedException);
            }catch (Exception exception){
                logger.error("Unknown Exception while storing tweet", exception);
                crisisMailer.sendEmailAlert(exception);
            }
        }
    }


    private void storeTweet(Status tweet){
        if (kijiConnection.isValidKijiConnection()){
            try{

                EntityId tweetId =
                        kijiConnection.kijiTable.getEntityId(String.valueOf(tweet.getId()));
                kijiConnection.kijiTableWriter.put(tweetId,
                                    GlobalConstants.TWEET_COLUMN_FAMILY_NAME,
                                    GlobalConstants.TWEET_COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    SimpleTweetGenerator.generateSimpleTweet(tweet));
                kijiConnection.kijiTableWriter.put(tweetId,
                                    GlobalConstants.TWEET_COLUMN_FAMILY_NAME,
                                    GlobalConstants.LABEL_COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    null);
            }catch (IOException ioException){
                logger.error("Exception while opening TableWriter" +
                            "or 'putting' a row",
                            ioException);
                crisisMailer.sendEmailAlert(ioException);
            }catch (Exception exception){
                logger.error("Unknown Exception while 'putting' tweet",
                        exception);
                crisisMailer.sendEmailAlert(exception);
            }
        }
    }
}
