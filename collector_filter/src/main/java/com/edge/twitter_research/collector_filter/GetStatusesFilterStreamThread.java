package com.edge.twitter_research.collector_filter;

import com.edge.twitter_research.core.CrisisMailer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;


public class GetStatusesFilterStreamThread extends Thread {

    private TwitterStream twitterStream;
    private String phraseToTrack;
    private static Logger logger =
            Logger.getLogger(GetStatusesFilterStreamThread.class);
    private CrisisMailer crisisMailer;

    public GetStatusesFilterStreamThread(TwitterStream twitterStream,
                                         String phraseToTrack,
                                         String log4jPropertiesFilePath){
        this.twitterStream = twitterStream;
        this.phraseToTrack = phraseToTrack;
        crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(log4jPropertiesFilePath);
    }

    public void run(){
        String[] trackArray = new String[1];
        trackArray[0] = phraseToTrack;

        long[] followArray = new long[0];

        try{
            twitterStream.filter(new FilterQuery(0, followArray, trackArray));
        }catch (Exception unknownException){
            logger.error("Unknown Exception while running a filtered stream",
                    unknownException);
            crisisMailer.sendEmailAlert(unknownException);
        }
    }
}
