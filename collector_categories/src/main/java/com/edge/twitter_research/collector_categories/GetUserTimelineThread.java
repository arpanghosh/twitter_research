package com.edge.twitter_research.collector_categories;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import org.apache.log4j.PropertyConfigurator;
import org.kiji.schema.KijiTableWriter;

import twitter4j.*;

import com.edge.twitter_research.core.*;


public class GetUserTimelineThread extends Thread {

    private TwitterFactory twitterFactory;
    private LinkedBlockingQueue<TweetCategoryMessage> outputQueue;
    private PriorityBlockingQueue<UserCategoryMessage> inputQueue;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(GetUserTimelineThread.class);

    private KijiConnection kijiConnection;
    private KijiTableWriter kijiTableWriter = null;




    public GetUserTimelineThread(TwitterFactory twitterFactory,
                                    PriorityBlockingQueue<UserCategoryMessage> inputQueue,
                                    LinkedBlockingQueue<TweetCategoryMessage> outputQueue,
                                    String tableLayoutPath,
                                    String tableName,
                                    String log4jPropertiesFilePath){
        this.twitterFactory = twitterFactory;
        this.outputQueue = outputQueue;
        this.inputQueue = inputQueue;
        this.kijiConnection = new KijiConnection(tableLayoutPath, tableName);
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(log4jPropertiesFilePath);

        try{
            if (kijiConnection.kijiTable != null){
                this.kijiTableWriter =
                        kijiConnection.kijiTable.openTableWriter();
            }
        }catch (IOException ioException){
            logger.error("Exception while opening KijiTableWriter",
                    ioException);
            crisisMailer.sendEmailAlert(ioException);
        }
    }


    public void run(){
        Twitter twitter = twitterFactory.getInstance();

        UserCategoryMessage lastUserCategoryMessage = null;
        UserCategoryMessage userCategoryMessage;

        while(true){
            try{
                userCategoryMessage = inputQueue.take();

                /*
                if (timeToStop(userCategoryMessage)){
                    outputQueue.add(new TweetCategoryMessage(null, ""));
                    break;
                }
                */

                if (userCategoryMessage.equals(lastUserCategoryMessage)){
                    continue;
                }
                lastUserCategoryMessage = userCategoryMessage;
            }catch (InterruptedException interruptedException){
                logger.warn("Exception while 'taking' item from queue",
                        interruptedException);
                continue;
            }

            boolean success = false;
            do{
                try{
                    Paging paging =
                            new Paging(1,
                                        Constants.MAX_TWEETS_IN_ONE_REQUEST,
                                        userCategoryMessage.since_id);

                    ResponseList<Status> statuses =
                            twitter.getUserTimeline(userCategoryMessage.user_id, paging);
                    success = true;

                    for (Status status : statuses){
                        //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                        outputQueue.add(new TweetCategoryMessage(status,
                                                                userCategoryMessage.category_slug));
                    }

                    if (statuses.size() > 0){
                        storeUserSinceId(userCategoryMessage.user_id,
                                        statuses.get(0).getId());
                    }
                }catch (TwitterException twitterException){
                    if (twitterException.exceededRateLimitation() &&
                            twitterException.getRateLimitStatus() != null){
                        logger.warn("GetUserTimelineThread Rate Limit Reached",
                                twitterException);
                        CollectorDriver.putToSleep(GlobalConstants.RATE_LIMIT_WINDOW);
                    }else{
                        logger.error("Exception while fetching tweets for a user from Twitter",
                                twitterException);
                        crisisMailer.sendEmailAlert(twitterException);
                        CollectorDriver
                                .putToSleep(GlobalConstants
                                            .BACKOFF_AFTER_TWITTER_API_FAILURE);
                    }
                }
            }while (!success);
        }
        //logger.error("GetUserTimelineThread ended");
    }


    private void storeUserSinceId(Long userId, Long since_id){
        if (kijiTableWriter != null){
            try{
                kijiTableWriter.put(kijiConnection.kijiTable
                                    .getEntityId(userId.toString()),
                                    Constants.LAST_TWEET_ID_COLUMN_FAMILY_NAME,
                                    Constants.LAST_TWEET_ID_COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    since_id);
            }catch (IOException ioException){
                logger.error("Exception while 'putting' a row in a KijiTable",
                        ioException);
                crisisMailer.sendEmailAlert(ioException);
            }
        }
    }

    private boolean timeToStop(UserCategoryMessage userCategoryMessage){
        return (userCategoryMessage.equals(new UserCategoryMessage(-1L, "", Long.MAX_VALUE)));
    }
}
