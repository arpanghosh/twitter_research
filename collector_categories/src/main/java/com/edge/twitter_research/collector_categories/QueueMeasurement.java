package com.edge.twitter_research.collector_categories;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class QueueMeasurement extends Thread {

    LinkedBlockingQueue<String> categories;
    PriorityBlockingQueue<UserCategoryMessage> users;
    LinkedBlockingQueue<TweetCategoryMessage> tweets;

    public QueueMeasurement(LinkedBlockingQueue<String> categories,
                            PriorityBlockingQueue<UserCategoryMessage> users,
                            LinkedBlockingQueue<TweetCategoryMessage> tweets){
        this.categories = categories;
        this.users = users;
        this.tweets = tweets;
    }

    public void run(){
        do{
            CollectorDriver.putToSleep(60);
            System.out.println("Categories: " + categories.size() +
                    "\tUsers: " + users.size() +
                    "\tTweets: " + tweets.size());
        }while (users.size() > 0 ||
                categories.size() > 0 ||
                tweets.size() > 0);
        System.out.println("QueueMeasurement ended");
    }
}
