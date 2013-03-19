package com.edge.twitter_research.collector_filter;

import twitter4j.*;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class GetStatusesFilterStreamListener implements StatusListener {

    private LinkedBlockingQueue<TweetPhraseMessage> outputQueue;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(GetStatusesFilterStreamListener.class);
    private String phrase;

    public GetStatusesFilterStreamListener(LinkedBlockingQueue<TweetPhraseMessage> outputQueue,
                                           String log4jPropertiesFilePath,
                                           String phrase){
        this.outputQueue = outputQueue;
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        this.phrase = phrase;
        PropertyConfigurator.configure(log4jPropertiesFilePath);
    }

    @Override
    public void onStatus(Status status) {
        //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
        outputQueue.add(new TweetPhraseMessage(status, phrase));
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        logger.warn("Got track limitation notice:" + numberOfLimitedStatuses);
        //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        logger.warn("Received a Stall Warning : " + warning.getMessage());
        //System.out.println("Got stall warning:" + warning);
    }

    @Override
    public void onException(Exception exception) {
        logger.error("Exception in Filter Stream Listener",
                    exception);
        crisisMailer.sendEmailAlert(exception);
    }
}
