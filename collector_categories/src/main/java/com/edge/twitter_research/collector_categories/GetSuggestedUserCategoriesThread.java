package com.edge.twitter_research.collector_categories;

import org.apache.log4j.PropertyConfigurator;
import twitter4j.*;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.edge.twitter_research.core.*;

public class GetSuggestedUserCategoriesThread extends Thread {

    private TwitterFactory twitterFactory;
    private LinkedBlockingQueue<String> outputQueue;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(GetSuggestedUserCategoriesThread.class);


    public GetSuggestedUserCategoriesThread(TwitterFactory twitterFactory,
                                            LinkedBlockingQueue<String> outputQueue){
        this.twitterFactory = twitterFactory;
        this.outputQueue = outputQueue;
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(this.getClass()
                .getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));
    }


    public void run(){
        Twitter twitter = twitterFactory.getInstance();

        while(true){
            try {
                ResponseList<Category> categories =
                        twitter.getSuggestedUserCategories();
                for (Category category : categories){
                    outputQueue.add(category.getSlug());
                }

                Timer.putToSleep(Math.max(Constants.COLLECTION_INTERVAL_IN_SECS,
                                    categories.getRateLimitStatus()
                                            .getSecondsUntilReset() + 1));

            } catch (TwitterException twitterException) {
                if (twitterException.exceededRateLimitation() &&
                        twitterException.getRateLimitStatus() != null){
                    logger.warn("GetSuggestedUserCategoriesThread Rate Limit Reached",
                            twitterException);
                    Timer.putToSleep(GlobalConstants.RATE_LIMIT_WINDOW);
                }else{
                    logger.error("Exception while fetching SuggestedUserCategories from Twitter",
                            twitterException);
                    crisisMailer.sendEmailAlert(twitterException);
                    Timer.putToSleep(GlobalConstants
                                    .BACKOFF_AFTER_TWITTER_API_FAILURE);
                }
            } catch (Exception unknownException){
                logger.error("Unknown Exception while fetching SuggestedUserCategories from Twitter",
                        unknownException);
                crisisMailer.sendEmailAlert(unknownException);
            }
        }
    }
}
