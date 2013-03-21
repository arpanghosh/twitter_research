package com.edge.twitter_research.collector_categories;

import org.apache.log4j.PropertyConfigurator;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiTable;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import  com.edge.twitter_research.core.*;

public class TweetStorageThread extends Thread {

    private LinkedBlockingQueue<TweetCategoryMessage> inputQueue;
    private KijiConnection kijiConnection;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(TweetStorageThread.class);
    private long tweetCounter = 0L;

    public TweetStorageThread(LinkedBlockingQueue<TweetCategoryMessage> inputQueue,
                              String tableLayoutPath,
                              String tableName,
                              String log4jPropertiesFilePath){
        this.inputQueue = inputQueue;
        this.kijiConnection = new KijiConnection(tableLayoutPath, tableName);
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(log4jPropertiesFilePath);
    }

    public void run(){
        TweetCategoryMessage tweetCategoryMessage;
        while(true){
            try{
                tweetCategoryMessage = inputQueue.take();

                storeTweet(tweetCategoryMessage);
                tweetCounter++;

                if (tweetCounter % 1000 == 0){
                    logger.warn(DateTimeCreator.getDateTimeString() +
                            " - Total Tweets so far : " + tweetCounter);
                }
            }catch (InterruptedException interruptedException){
                logger.warn("Exception while 'taking' item from queue",
                        interruptedException);
            }
        }
    }


    private void storeTweet(TweetCategoryMessage tweetCategoryMessage){
        if (kijiConnection.isValidKijiConnection()){
            try{
                EntityId tweetId =
                        kijiConnection.kijiTable.getEntityId(tweetCategoryMessage.category_slug,
                                tweetCategoryMessage.tweet.getId());
                kijiConnection.kijiTableWriter.put(tweetId,
                                    GlobalConstants.TWEET_COLUMN_FAMILY_NAME,
                                    GlobalConstants.TWEET_COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    SimpleTweetGenerator.generateSimpleTweet(tweetCategoryMessage.tweet));
                kijiConnection.kijiTableWriter.put(tweetId,
                                    GlobalConstants.TWEET_COLUMN_FAMILY_NAME,
                                    GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    null);
                kijiConnection.kijiTableWriter.put(tweetId,
                                    GlobalConstants.TWEET_COLUMN_FAMILY_NAME,
                                    GlobalConstants.TOPIC_LABEL_COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    tweetCategoryMessage.category_slug);
            }catch (IOException ioException){
                logger.error("Exception while 'putting' tweet in KijiTable",
                        ioException);
                crisisMailer.sendEmailAlert(ioException);
            }catch (Exception unknownException){
                logger.error("Unknown Exception while 'putting' tweet in KijiTable",
                        unknownException);
                crisisMailer.sendEmailAlert(unknownException);
            }
        }
    }


    private boolean timeToStop(TweetCategoryMessage tweetCategoryMessage){
        return (tweetCategoryMessage.category_slug.equals("") &&
                tweetCategoryMessage.tweet == null);
    }
}
