package com.edge.twitter_research.collector_filter;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;


public class GetStatusesFilterStreamThread extends Thread {

    private TwitterStream twitterStream;
    private String phraseToTrack;

    public GetStatusesFilterStreamThread(TwitterStream twitterStream,
                                         String phraseToTrack){
        this.twitterStream = twitterStream;
        this.phraseToTrack = phraseToTrack;
    }

    public void run(){
        String[] trackArray = new String[1];
        trackArray[0] = phraseToTrack;

        long[] followArray = new long[0];

        twitterStream.filter(new FilterQuery(0, followArray, trackArray));
    }

}
