package com.edge.twitter_research.collector_categories;

import twitter4j.Status;

public class TweetCategoryMessage {

    Status tweet;
    String category_slug;

    public TweetCategoryMessage(Status tweet, String category_slug){
        this.tweet = tweet;
        this.category_slug = category_slug;
    }
}
