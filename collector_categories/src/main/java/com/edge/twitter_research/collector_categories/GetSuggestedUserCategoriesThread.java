package com.edge.twitter_research.collector_categories;

import twitter4j.*;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

public class GetSuggestedUserCategoriesThread extends Thread {

    private TwitterFactory twitterFactory;
    private LinkedBlockingQueue<String> outputQueue;
    private CrisisMailer crisisMailer;


    public GetSuggestedUserCategoriesThread(TwitterFactory twitterFactory,
                                            LinkedBlockingQueue<String> outputQueue){
        this.twitterFactory = twitterFactory;
        this.outputQueue = outputQueue;
        this.crisisMailer = CrisisMailer.getCrisisMailer();
    }


    public void run(){
        Twitter twitter = twitterFactory.getInstance();

        //while(true){
            try {
                ResponseList<Category> categories =
                        twitter.getSuggestedUserCategories();
                for (Category category : categories){
                    outputQueue.add(category.getSlug());
                }

                CollectorDriver
                        .putToSleep(Math.max(Constants.COLLECTION_INTERVAL_IN_SECS,
                                    categories.getRateLimitStatus()
                                            .getSecondsUntilReset() + 1));

            } catch (TwitterException twitterException) {
                if (twitterException.exceededRateLimitation() &&
                        twitterException.getRateLimitStatus() != null){
                    System.out.println("GetSuggestedUserCategoriesThread" + " Rate Limit Reached");

                    CollectorDriver.putToSleep(Constants.RATE_LIMIT_WINDOW);
                }else{
                    twitterException.printStackTrace();
                    crisisMailer.sendEmailAlert(twitterException);
                    CollectorDriver
                            .putToSleep(Constants
                                    .BACKOFF_AFTER_TWITTER_API_FAILURE);
                }
            }
            outputQueue.add("END");
            System.out.println("GetSuggestedUserCategoriesThread ended");
        //}
    }
}
