package com.edge.twitter_research.collector_streaming;

import twitter4j.TwitterStream;


public class GetStatusesSampleStreamThread extends Thread {

    private TwitterStream twitterStream;

    public GetStatusesSampleStreamThread(TwitterStream twitterStream){
        this.twitterStream = twitterStream;
    }

    public void run(){
        twitterStream.sample();
    }

}
