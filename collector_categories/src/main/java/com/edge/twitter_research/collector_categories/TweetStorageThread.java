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
                              String tableName){
        this.inputQueue = inputQueue;
        this.kijiConnection =
                new KijiConnection(this.getClass()
                                    .getResourceAsStream(Constants
                                            .CATEGORY_TWEET_STORE_TABLE_LAYOUT_FILE_NAME),
                                                        tableName);
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(this.getClass()
                .getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));
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
            }catch (IOException ioException){
                logger.error("IOException while storing tweet",
                        ioException);
                crisisMailer.sendEmailAlert(ioException);
            }catch (Exception exception){
                logger.error("Unknown Exception while storing tweet",
                        exception);
                crisisMailer.sendEmailAlert(exception);
            }
        }
    }


    private void storeTweet(TweetCategoryMessage tweetCategoryMessage)
        throws IOException{
        if (kijiConnection.isValidKijiConnection()){
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
        }
    }


    private boolean timeToStop(TweetCategoryMessage tweetCategoryMessage){
        return (tweetCategoryMessage.category_slug.equals("") &&
                tweetCategoryMessage.tweet == null);
    }
}
