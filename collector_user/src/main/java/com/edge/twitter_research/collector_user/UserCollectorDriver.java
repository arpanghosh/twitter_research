package com.edge.twitter_research.collector_user;


import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class UserCollectorDriver {

    private static Logger logger =
            Logger.getLogger(UserCollectorDriver.class);
    private static CrisisMailer crisisMailer =
            CrisisMailer.getCrisisMailer();


    public static void main(String[] args){

        if (args.length < 1){
            System.out.println("Usage: UserCollectorDriver " +
                    "<user_file_path>");
            System.exit(-1);
        }

        String userFilePath = args[0];

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{

            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(Constants.OAUTH_CONSUMER_KEY)
                .setOAuthConsumerSecret(Constants.OAUTH_CONSUMER_SECRET)
                .setOAuthAccessToken(Constants.OAUTH_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Constants.OAUTH_ACCESS_TOKEN_SECRET);
            TwitterFactory twitterFactory =
                    new TwitterFactory(configurationBuilder.build());

            LinkedBlockingQueue<TweetCompanyMessage> tweetStorageQueue =
                new LinkedBlockingQueue<TweetCompanyMessage>();

            LinkedBlockingQueue<UserCompanyMessage> usersToFetchTweetsForQueue =
                    new LinkedBlockingQueue<UserCompanyMessage>();


            Thread tweetStorageThread =
                new TweetStorageThread(tweetStorageQueue,
                                        GlobalConstants.USER_TWEET_STORAGE_TABLE_NAME);

            Thread phraseFetchingThread =
                new UserFetchingThread(userFilePath,
                                        usersToFetchTweetsForQueue);

            Thread getUserTimelineThread =
                    new GetUserTimelineThread(twitterFactory,
                                                usersToFetchTweetsForQueue,
                                                tweetStorageQueue);

            tweetStorageThread.start();
            getUserTimelineThread.start();
            phraseFetchingThread.start();


            phraseFetchingThread.join();
            getUserTimelineThread.join();
            tweetStorageThread.join();


        }catch (InterruptedException interruptedException){
            logger.warn("Exception while Collector threads are joining",
                        interruptedException);
        }catch (Exception unknownException){
            logger.error("Unknown Exception while collector_filter is starting",
                    unknownException);
            crisisMailer.sendEmailAlert(unknownException);
        }

        logger.error("FilterCollectorDriver has stopped of own free will");
        crisisMailer.sendEmailAlert("collector_filter: FilterCollectorDriver has stopped of own free will");
    }
}
