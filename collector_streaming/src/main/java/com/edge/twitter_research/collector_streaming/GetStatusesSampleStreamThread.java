package com.edge.twitter_research.collector_streaming;

import com.edge.twitter_research.core.CrisisMailer;
import twitter4j.TwitterStream;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class GetStatusesSampleStreamThread extends Thread {

    private TwitterStream twitterStream;
    private static Logger logger =
            Logger.getLogger(GetStatusesSampleStreamThread.class);
    private CrisisMailer crisisMailer;

    public GetStatusesSampleStreamThread(TwitterStream twitterStream){
        this.twitterStream = twitterStream;
        PropertyConfigurator.configure(this.getClass().getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));
        this.crisisMailer = CrisisMailer.getCrisisMailer();
    }

    public void run(){
        try{
            twitterStream.sample();
        }catch (Exception exception){
            logger.error("Exception while sampling twitter stream", exception);
            crisisMailer.sendEmailAlert(exception);
        }
    }
}
