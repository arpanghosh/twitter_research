package com.edge.twitter_research.collector_streaming;

import twitter4j.*;

import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class GetStatusesSampleStreamListener implements StatusListener {

    private LinkedBlockingQueue<Status> outputQueue;
    private CrisisMailer crisisMailer;
    private static Logger logger =
            Logger.getLogger(GetStatusesSampleStreamListener.class);

    public GetStatusesSampleStreamListener(LinkedBlockingQueue<Status> outputQueue,
                                           String log4jPropertiesFilePath){
        try{
            this.outputQueue = outputQueue;
            this.crisisMailer = CrisisMailer.getCrisisMailer();
            PropertyConfigurator.configure(log4jPropertiesFilePath);
        }catch (Exception exception){
            logger.error("Exception while setting up GetStatusesSampleStreamListener",
                    exception);
            crisisMailer.sendEmailAlert(exception);
        }
    }

    @Override
    public void onStatus(Status status) {
        //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
        try{
            outputQueue.add(status);
        }catch (Exception exception){
            logger.error("Exception while adding tweet to storage queue", exception);
            crisisMailer.sendEmailAlert(exception);
        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        logger.warn("Got track limitation notice:" + numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        logger.warn("Got a stall warning: " + warning.getMessage());
    }

    @Override
    public void onException(Exception exception) {
        logger.error("Exception in Sample Stream Listener",
                        exception);
        crisisMailer.sendEmailAlert(exception);
    }
}
