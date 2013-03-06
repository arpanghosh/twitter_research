package com.edge.twitter_research.collector_categories;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.kiji.schema.KijiTableWriter;

import twitter4j.*;

import com.edge.twitter_research.core.*;


public class GetUserTimelineThread extends Thread {

    private TwitterFactory twitterFactory;
    private LinkedBlockingQueue<TweetCategoryMessage> outputQueue;
    private PriorityBlockingQueue<UserCategoryMessage> inputQueue;
    private CrisisMailer crisisMailer;

    private KijiConnection kijiConnection;
    private KijiTableWriter kijiTableWriter = null;

    private static final String COLUMN_FAMILY_NAME = "last_since_id";
    private static final String COLUMN_NAME = "since_id";


    public GetUserTimelineThread(TwitterFactory twitterFactory,
                                    PriorityBlockingQueue<UserCategoryMessage> inputQueue,
                                    LinkedBlockingQueue<TweetCategoryMessage> outputQueue,
                                    String tableLayoutPath,
                                    String tableName){
        this.twitterFactory = twitterFactory;
        this.outputQueue = outputQueue;
        this.inputQueue = inputQueue;
        this.kijiConnection = new KijiConnection(tableLayoutPath, tableName);
        this.crisisMailer = CrisisMailer.getCrisisMailer();

        try{
            if (kijiConnection.kijiTable != null){
                this.kijiTableWriter =
                        kijiConnection.kijiTable.openTableWriter();
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
            crisisMailer.sendEmailAlert(ioException);
        }
    }


    public void run(){
        Twitter twitter = twitterFactory.getInstance();

        UserCategoryMessage lastUserCategoryMessage = null;
        UserCategoryMessage userCategoryMessage;

        int numRequests = 0;
        while(true){
            try{
                userCategoryMessage = inputQueue.take();
                if (timeToStop(userCategoryMessage)){
                    outputQueue.add(new TweetCategoryMessage(null, ""));
                    break;
                }

                if (userCategoryMessage.equals(lastUserCategoryMessage)){
                    continue;
                }
                lastUserCategoryMessage = userCategoryMessage;
            }catch (InterruptedException interruptedException){
                interruptedException.printStackTrace();
                continue;
            }

            for (int i = 1; i <= 10; i++){
                try{
                    Paging paging =
                            new Paging(i,
                                        Constants.MAX_TWEETS_IN_ONE_REQUEST,
                                        userCategoryMessage.since_id);

                    ResponseList<Status> statuses =
                            twitter.getUserTimeline(userCategoryMessage.user_id, paging);
                    numRequests++;

                    if (statuses.isEmpty()){
                        //System.out.println("No new tweets");
                        break;
                    }

                    for (Status status : statuses){
                        outputQueue.add(new TweetCategoryMessage(status,
                                                                userCategoryMessage.category_slug));
                        //System.out.println("Fetched statuses for " + userCategoryMessage.user_id);
                    }

                    if (i == 1){
                        storeUserSinceId(userCategoryMessage.user_id, statuses.get(0).getId());
                    }

                }catch (TwitterException twitterException){
                    if (twitterException.exceededRateLimitation() &&
                            twitterException.getRateLimitStatus() != null){
                        System.out.println("GetUserTimelineThread" + " Rate Limit Reached");
                        System.out.println("Number of requests in this window: " + numRequests);
                        CollectorDriver.putToSleep(GlobalConstants.RATE_LIMIT_WINDOW);
                        i--;
                        numRequests = 0;
                    }else{
                        twitterException.printStackTrace();
                        crisisMailer.sendEmailAlert(twitterException);
                        CollectorDriver
                                .putToSleep(GlobalConstants
                                            .BACKOFF_AFTER_TWITTER_API_FAILURE);
                    }
                }
            }
        }
        System.out.println("GetUserTimelineThread ended");
    }


    private void storeUserSinceId(Long userId, Long since_id){
        if (kijiTableWriter != null){
            try{
                kijiTableWriter.put(kijiConnection.kijiTable
                                    .getEntityId(userId.toString()),
                                    COLUMN_FAMILY_NAME,
                                    COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    since_id);
            }catch (IOException ioException){
                ioException.printStackTrace();
                crisisMailer.sendEmailAlert(ioException);
            }
        }
    }

    private boolean timeToStop(UserCategoryMessage userCategoryMessage){
        return (userCategoryMessage.equals(new UserCategoryMessage(-1L, "", Long.MAX_VALUE)));
    }
}
