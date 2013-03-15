package com.edge.twitter_research.collector_filter;

import twitter4j.Status;

public class TweetPhraseMessage {

    Status tweet;
    String phrase;

    public TweetPhraseMessage(Status tweet, String phrase){
        this.tweet = tweet;
        this.phrase = phrase;
    }
}
