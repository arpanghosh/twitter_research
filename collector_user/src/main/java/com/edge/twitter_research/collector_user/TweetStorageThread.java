package com.edge.twitter_research.collector_user;

import org.apache.log4j.PropertyConfigurator;
import org.kiji.schema.EntityId;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import  com.edge.twitter_research.core.*;

public class TweetStorageThread extends Thread {

    private LinkedBlockingQueue<TweetCompanyMessage> inputQueue;
    private KijiConnection kijiConnection;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(TweetStorageThread.class);
    private long tweetCounter = 0L;

    public TweetStorageThread(LinkedBlockingQueue<TweetCompanyMessage> inputQueue,
                              String tableName){
        this.inputQueue = inputQueue;
        this.kijiConnection
                = new KijiConnection(this.getClass()
                                    .getResourceAsStream(Constants
                                            .USER_TWEET_STORE_TABLE_LAYOUT_FILE_NAME), tableName);
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);
    }

    public void run(){
        TweetCompanyMessage tweetCompanyMessage;
        while(true){
            try{
                tweetCompanyMessage = inputQueue.take();
                storeTweet(tweetCompanyMessage);
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


    private void storeTweet(TweetCompanyMessage tweetCompanyMessage)
        throws IOException{
        if (kijiConnection.isValidKijiConnection()){
                EntityId entityId =
                        kijiConnection.kijiTable.getEntityId(tweetCompanyMessage
                                                            .tweet.getUser().getId(),
                                                            tweetCompanyMessage.companyData.getCompanyName(),
                                                            tweetCompanyMessage.companyData.getCompanyArea());
                kijiConnection.kijiTableWriter.put(entityId,
                        GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.TWEET_COLUMN_NAME,
                        tweetCompanyMessage.tweet.getId(),
                        SimpleTweetGenerator.generateSimpleTweet(tweetCompanyMessage.tweet));
                kijiConnection.kijiTableWriter.put(entityId,
                    GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                    GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                    tweetCompanyMessage.tweet.getId(),
                    null);
                kijiConnection.kijiTableWriter.put(entityId,
                    GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                    GlobalConstants.TOPIC_LABEL_COLUMN_NAME,
                    tweetCompanyMessage.tweet.getId(),
                    null);
        }
    }
}
