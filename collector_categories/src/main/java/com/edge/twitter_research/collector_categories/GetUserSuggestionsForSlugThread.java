package com.edge.twitter_research.collector_categories;

import org.apache.log4j.PropertyConfigurator;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;

import twitter4j.*;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import com.edge.twitter_research.core.*;


public class GetUserSuggestionsForSlugThread extends Thread {

    private static Logger logger =
            Logger.getLogger(GetUserSuggestionsForSlugThread.class);

    private TwitterFactory twitterFactory;
    private PriorityBlockingQueue<UserCategoryMessage> outputQueue;
    private LinkedBlockingQueue<String> inputQueue;
    private CrisisMailer crisisMailer;

    private KijiConnection kijiConnection;


    public GetUserSuggestionsForSlugThread(TwitterFactory twitterFactory,
                                           LinkedBlockingQueue<String> inputQueue,
                                            PriorityBlockingQueue<UserCategoryMessage> outputQueue,
                                            String tableLayoutPath,
                                            String tableName,
                                            String log4jPropertiesFilePath){
        this.twitterFactory = twitterFactory;
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.crisisMailer = CrisisMailer.getCrisisMailer();
        PropertyConfigurator.configure(log4jPropertiesFilePath);

        this.kijiConnection = new KijiConnection(tableLayoutPath, tableName);
    }


    public void run(){
        Twitter twitter = twitterFactory.getInstance();

        String slug;
        while(true){

            try{
                slug = inputQueue.take();
                /*
                if(slug.equals("END")){
                    outputQueue.add(new UserCategoryMessage(-1L, "", Long.MAX_VALUE));
                    break;
                }
                */

            }catch (InterruptedException interruptedException){
                logger.warn("Exception while 'taking' element from queue",
                        interruptedException);
                continue;
            }

            boolean success = false;
            do{
                try{
                    ResponseList<User> users =
                            twitter.getUserSuggestions(slug);
                    success = true;
                    for (User user : users){
                        if (!user.isProtected()){
                            outputQueue.add(new UserCategoryMessage(user.getId(),
                                                                    slug,
                                                                    getUserSinceId(user.getId())));
                        }
                    }

                }catch (TwitterException twitterException){
                    if (twitterException.exceededRateLimitation() &&
                            twitterException.getRateLimitStatus() != null){
                        logger.warn("GetUserSuggestionsForSlugThread Rate Limit Reached",
                                twitterException);
                        CollectorDriver.putToSleep(GlobalConstants.RATE_LIMIT_WINDOW);
                    }else{
                        logger.error("Exception while fetching users for a Slug from Twitter",
                                twitterException);
                        crisisMailer.sendEmailAlert(twitterException);
                        CollectorDriver
                                .putToSleep(GlobalConstants
                                        .BACKOFF_AFTER_TWITTER_API_FAILURE);
                    }
                }catch (Exception unknownException){
                    logger.error("Unknown Exception while fetching users for a Slug from Twitter",
                            unknownException);
                    crisisMailer.sendEmailAlert(unknownException);
                }

            }while (!success);
        }
        //logger.error("GetUserSuggestionsForSlugThread ended");
    }


    private Long getUserSinceId(Long userId){
        Long since_id = 1L;

        if (kijiConnection.isValidKijiConnection()){
            try{
                KijiRowData since =
                        kijiConnection.kijiTableReader.get(kijiConnection.kijiTable
                                .getEntityId(userId),
                                KijiDataRequest.create(Constants.LAST_TWEET_ID_COLUMN_FAMILY_NAME,
                                        Constants.LAST_TWEET_ID_COLUMN_NAME));
                if (since.containsColumn(Constants.LAST_TWEET_ID_COLUMN_FAMILY_NAME,
                                        Constants.LAST_TWEET_ID_COLUMN_NAME)){
                    since_id = since.getMostRecentValue(Constants.LAST_TWEET_ID_COLUMN_FAMILY_NAME,
                                                        Constants.LAST_TWEET_ID_COLUMN_NAME);
                }
            }catch (IOException ioException){
                logger.error("Exception while 'getting' row using KijiTableReader",
                        ioException);
                crisisMailer.sendEmailAlert(ioException);
            }catch (Exception unknownException){
                logger.error("Unknown Exception while 'getting' row using KijiTableReader",
                        unknownException);
                crisisMailer.sendEmailAlert(unknownException);
            }
        }
        return since_id;
    }
}
