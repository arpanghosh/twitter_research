package com.edge.twitter_research.collector_categories;

import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import com.edge.twitter_research.core.*;


public class CollectorDriver {

    public static void putToSleep(int seconds){
        boolean slept = false;
        do{
            try{
                Thread.sleep(seconds * 1000);
                slept = true;
            }catch (Exception exception){
                exception.printStackTrace();
            }
        }while(!slept);
    }


    public static void main(String[] args){

        if (args.length < 2){
            System.out.println("Usage: CollectorDriver " +
                    "category_tweet_store_layout file path> " +
                    "<users_last_tweet_id_layout file path");
            return;
        }

        String categoryTweetStoreLayoutFilePath = args[0];
        String usersLastTweetIdLayoutFilePath = args[1];


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
                                                    usersLastTweetIdLayoutFilePath,
                                                    Constants.USER_LAST_TWEET_ID_TABLE_NAME);
        Thread tweetsForUserThread =
                new GetUserTimelineThread(twitterFactory,
                                            userFetchingQueue,
                                            tweetStorageQueue,
                                            usersLastTweetIdLayoutFilePath,
                                            Constants.USER_LAST_TWEET_ID_TABLE_NAME);
        Thread tweetStorageThread =
                new TweetStorageThread(tweetStorageQueue,
                                        categoryTweetStoreLayoutFilePath,
                                        Constants.TWEET_STORAGE_TABLE_NAME);

        Thread queueMeasurementThread =
                new QueueMeasurement(categoryFetchingQueue,
                                        userFetchingQueue,
                                        tweetStorageQueue);


        //userFetchingQueue.add(new UserCategoryMessage(111757158L, "nascar"));

        long tic = System.currentTimeMillis();

        suggestedCategoryThread.start();
        usersInCategoryThread.start();
        tweetsForUserThread.start();
        tweetStorageThread.start();
        queueMeasurementThread.start();



        try{
            suggestedCategoryThread.join();
            usersInCategoryThread.join();
            tweetsForUserThread.join();
            tweetStorageThread.join();
            queueMeasurementThread.join();
        }catch (InterruptedException interruptedException){
            interruptedException.printStackTrace();
        }

        long toc = System.currentTimeMillis();

        System.out.print("\nAll threads joined. Total time: \n"
                        + (toc - tic)/3600000 +
                        " hours");
    }
}
