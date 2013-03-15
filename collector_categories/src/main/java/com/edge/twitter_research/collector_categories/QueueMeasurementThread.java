package com.edge.twitter_research.collector_categories;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class QueueMeasurementThread extends Thread {

    private LinkedBlockingQueue<String> categories;
    private PriorityBlockingQueue<UserCategoryMessage> users;
    private LinkedBlockingQueue<TweetCategoryMessage> tweets;
    private static Logger logger =
            Logger.getLogger(QueueMeasurementThread.class);

    public QueueMeasurementThread(LinkedBlockingQueue<String> categories,
                            PriorityBlockingQueue<UserCategoryMessage> users,
                            LinkedBlockingQueue<TweetCategoryMessage> tweets,
                            String log4jPropertiesFilePath){
        this.categories = categories;
        this.users = users;
        this.tweets = tweets;

        PropertyConfigurator.configure(log4jPropertiesFilePath);
    }

    public void run(){
        while (true){
            CollectorDriver.putToSleep(Constants.QUEUE_MEASUREMENT_INTERVAL_IN_SECS);
            logger.info("Categories: " + categories.size() +
                    "\tUsers: " + users.size() +
                    "\tTweets: " + tweets.size());
        }
        //logger.error("QueueMeasurementThread ended");
    }
}
