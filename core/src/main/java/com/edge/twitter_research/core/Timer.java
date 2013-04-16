package com.edge.twitter_research.core;


import org.apache.log4j.Logger;

public class Timer {

    private static Logger logger =
            Logger.getLogger(CrisisMailer.class);

    public static void putToSleep(int seconds){
        boolean slept = false;
        do{
            try{
                Thread.sleep(seconds * 1000);
                slept = true;
            }catch (InterruptedException interruptedException){
                logger.warn("Exception while trying to sleep",
                        interruptedException);
            }
        }while(!slept);
    }
}
