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
            }catch (InterruptedException interruptedException){
                logger.warn("Exception while taking an item from the queue",
                        interruptedException);
            }
        }
    }


    private void storeTweet(Status tweet){
        KijiTable kijiTable = kijiConnection.kijiTable;
        if (kijiTable != null){
            try{
                KijiTableWriter kijiTableWriter =
                        kijiTable.openTableWriter();
                EntityId tweetId =
                        kijiTable.getEntityId(String.valueOf(tweet.getId()));
                kijiTableWriter.put(tweetId,
                                    GlobalConstants.TWEET_COLUMN_FAMILY_NAME,
                                    GlobalConstants.TWEET_COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    SimpleTweetGenerator.generateSimpleTweet(tweet));
                kijiTableWriter.put(tweetId,
                                    GlobalConstants.TWEET_COLUMN_FAMILY_NAME,
                                    GlobalConstants.LABEL_COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    null);
            }catch (IOException ioException){
                logger.error("Exception while opening TableWriter" +
                            "or 'putting' a row",
                            ioException);
                crisisMailer.sendEmailAlert(ioException);
            }
        }
    }
}
