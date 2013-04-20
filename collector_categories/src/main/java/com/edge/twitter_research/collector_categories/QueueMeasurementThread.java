package com.edge.twitter_research.collector_categories;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import com.edge.twitter_research.core.Timer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.edge.twitter_research.core.DateTimeCreator;

public class QueueMeasurementThread extends Thread {

    private LinkedBlockingQueue<String> categories;
    private PriorityBlockingQueue<UserCategoryMessage> users;
    private LinkedBlockingQueue<TweetCategoryMessage> tweets;
    private static Logger logger =
            Logger.getLogger(QueueMeasurementThread.class);

    public QueueMeasurementThread(LinkedBlockingQueue<String> categories,
                            PriorityBlockingQueue<UserCategoryMessage> users,
                            LinkedBlockingQueue<TweetCategoryMessage> tweets){
        this.categories = categories;
        this.users = users;
        this.tweets = tweets;

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);
    }

    public void run(){
        while (true){
            Timer.putToSleep(Constants.QUEUE_MEASUREMENT_INTERVAL_IN_SECS);
            logger.info(DateTimeCreator.getDateTimeString() + " - " +
                    "Categories: " + categories.size() +
                    "\tUsers: " + users.size() +
                    "\tTweets: " + tweets.size());
        }
    }
}
