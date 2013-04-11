package com.edge.twitter_research.collector_categories;

import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.edge.twitter_research.core.*;


public class CategoryCollectorDriver {

    private static Logger logger =
            Logger.getLogger(CategoryCollectorDriver.class);
    private static CrisisMailer crisisMailer =
            CrisisMailer.getCrisisMailer();

    public static void putToSleep(int seconds){
        boolean slept = false;
        do{
            try{
                Thread.sleep(seconds * 1000);
                slept = true;
            }catch (InterruptedException interruptedException){
                logger.warn("Exception while trying to sleep",
                        interruptedException);
            }
        }while(!slept);
    }


    public static void main(String[] args){

        PropertyConfigurator.configure(CategoryCollectorDriver.class.getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));

        try{

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Constants.OAUTH_CONSUMER_KEY)
                .setOAuthConsumerSecret(Constants.OAUTH_CONSUMER_SECRET)
                .setOAuthAccessToken(Constants.OAUTH_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Constants.OAUTH_ACCESS_TOKEN_SECRET);
        TwitterFactory twitterFactory =
                new TwitterFactory(configurationBuilder.build());

        LinkedBlockingQueue<String> categoryFetchingQueue =
                new LinkedBlockingQueue<String>();
        PriorityBlockingQueue<UserCategoryMessage> userFetchingQueue =
                new PriorityBlockingQueue<UserCategoryMessage>();
        LinkedBlockingQueue<TweetCategoryMessage> tweetStorageQueue =
                new LinkedBlockingQueue<TweetCategoryMessage>();

        Thread suggestedCategoryThread =
                new GetSuggestedUserCategoriesThread(twitterFactory,
                                                    categoryFetchingQueue);
        Thread usersInCategoryThread =
                new GetUserSuggestionsForSlugThread(twitterFactory,
                                                    categoryFetchingQueue,
                                                    userFetchingQueue,
                                                    GlobalConstants.USER_LAST_TWEET_ID_TABLE_NAME);
        Thread tweetsForUserThread =
                new GetUserTimelineThread(twitterFactory,
                                            userFetchingQueue,
                                            tweetStorageQueue,
                                            GlobalConstants.USER_LAST_TWEET_ID_TABLE_NAME);
        Thread tweetStorageThread =
                new TweetStorageThread(tweetStorageQueue,
                                        GlobalConstants.CATEGORY_TWEET_STORAGE_TABLE_NAME);

        Thread queueMeasurementThread =
                new QueueMeasurementThread(categoryFetchingQueue,
                                        userFetchingQueue,
                                        tweetStorageQueue);


        suggestedCategoryThread.start();
        usersInCategoryThread.start();
        tweetsForUserThread.start();
        tweetStorageThread.start();
        queueMeasurementThread.start();

            suggestedCategoryThread.join();
            usersInCategoryThread.join();
            tweetsForUserThread.join();
            tweetStorageThread.join();
            queueMeasurementThread.join();



        }catch (InterruptedException interruptedException){
            logger.warn("Exception while collector threads are joining",
                    interruptedException);
        }catch (Exception unknownException){
            logger.error("Unknown Exception while starting collector_categories",
                    unknownException);
            crisisMailer.sendEmailAlert(unknownException);
        }


        logger.error("CategoryCollectorDriver has stopped of own free will");
        crisisMailer.sendEmailAlert("collector_categories: CategoryCollectorDriver has stopped of own free will");
    }
}
