package com.edge.twitter_research.collector_user;


import com.edge.twitter_research.core.CrisisMailer;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.Timer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import twitter4j.*;

import java.util.concurrent.LinkedBlockingQueue;

public class GetUserTimelineThread extends Thread {

    private TwitterFactory twitterFactory;
    private LinkedBlockingQueue<TweetCompanyMessage> outputQueue;
    private LinkedBlockingQueue<UserCompanyMessage> inputQueue;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(GetUserTimelineThread.class);


    public GetUserTimelineThread(TwitterFactory twitterFactory,
                                 LinkedBlockingQueue<UserCompanyMessage> inputQueue,
                                 LinkedBlockingQueue<TweetCompanyMessage> outputQueue){
        this.twitterFactory = twitterFactory;
        this.outputQueue = outputQueue;
        this.inputQueue = inputQueue;
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);
    }


    public void run(){
        Twitter twitter = twitterFactory.getInstance();

        UserCompanyMessage userCompanyMessage;

        while(true){
            try{
                userCompanyMessage = inputQueue.take();
            }catch (InterruptedException interruptedException){
                logger.warn("Exception while 'taking' item from queue",
                        interruptedException);
                continue;
            }

            boolean success = false;
            do{
                try{

                    for (int i = 1; i <= 5; i++){
                        Paging paging = new Paging(i,
                                                    Constants.MAX_TWEETS_IN_ONE_REQUEST,
                                                    1L);

                        ResponseList<Status> statuses =
                            twitter.getUserTimeline(userCompanyMessage.userID, paging);

                        for (Status status : statuses){
                            outputQueue.add(new TweetCompanyMessage(status,
                                        userCompanyMessage.companyData));
                        }
                    }
                    success = true;

                }catch (TwitterException twitterException){
                    if (twitterException.exceededRateLimitation() &&
                            twitterException.getRateLimitStatus() != null){
                        logger.warn("GetUserTimelineThread Rate Limit Reached",
                                twitterException);
                        Timer.putToSleep(GlobalConstants.RATE_LIMIT_WINDOW);
                    }else{
                        logger.error("Exception while fetching tweets for a user from Twitter",
                                twitterException);
                        crisisMailer.sendEmailAlert(twitterException);
                        Timer.putToSleep(GlobalConstants
                                .BACKOFF_AFTER_TWITTER_API_FAILURE);
                    }
                }catch (Exception unknownException){
                    logger.error("Unknown Exception while fetching tweets for a user from Twitter",
                            unknownException);
                    crisisMailer.sendEmailAlert(unknownException);
                }

            }while (!success);
        }
    }
}

