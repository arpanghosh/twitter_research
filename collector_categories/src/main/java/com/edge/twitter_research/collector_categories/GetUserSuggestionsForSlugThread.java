package com.edge.twitter_research.collector_categories;

import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;
import twitter4j.*;

import java.io.IOException;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import com.edge.twitter_research.core.*;


public class GetUserSuggestionsForSlugThread extends Thread {

    private static final String COLUMN_FAMILY_NAME = "last_since_id";
    private static final String COLUMN_NAME = "since_id";

    private TwitterFactory twitterFactory;
    private PriorityBlockingQueue<UserCategoryMessage> outputQueue;
    private LinkedBlockingQueue<String> inputQueue;
    private CrisisMailer crisisMailer;

    private KijiConnection kijiConnection;
    private KijiTableReader kijiTableReader = null;


    public GetUserSuggestionsForSlugThread(TwitterFactory twitterFactory,
                                           LinkedBlockingQueue<String> inputQueue,
                                            PriorityBlockingQueue<UserCategoryMessage> outputQueue,
                                            String tableLayoutPath,
                                            String tableName){
        this.twitterFactory = twitterFactory;
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.crisisMailer = CrisisMailer.getCrisisMailer();

        this.kijiConnection = new KijiConnection(tableLayoutPath, tableName);
        try{
            if (kijiConnection.kijiTable != null){
                this.kijiTableReader =
                        kijiConnection.kijiTable.openTableReader();
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
            crisisMailer.sendEmailAlert(ioException);
        }
    }


    public void run(){
        Twitter twitter = twitterFactory.getInstance();

        String slug;
        while(true){

            try{
                slug = inputQueue.take();
                if(slug.equals("END")){
                    outputQueue.add(new UserCategoryMessage(-1L, "", Long.MAX_VALUE));
                    break;
                }

            }catch (InterruptedException interruptedException){
                interruptedException.printStackTrace();
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
                            //System.out.println("User: " + user.getScreenName())
                        }
                    }

                }catch (TwitterException twitterException){
                    if (twitterException.exceededRateLimitation() &&
                            twitterException.getRateLimitStatus() != null){
                        System.out.println("GetUserSuggestionsForSlugThread" + " Rate Limit Reached");
                        CollectorDriver.putToSleep(Constants.RATE_LIMIT_WINDOW);
                    }else{
                        twitterException.printStackTrace();
                        crisisMailer.sendEmailAlert(twitterException);
                        CollectorDriver
                                .putToSleep(Constants
                                        .BACKOFF_AFTER_TWITTER_API_FAILURE);
                    }
                }
            }while (!success);
            System.out.println("GetUserSuggestionsForSlugThread ended");
        }
    }


    private Long getUserSinceId(Long userId){
        Long since_id = 1L;

        if (kijiTableReader != null){
            try{
                KijiRowData since =
                        kijiTableReader.get(kijiConnection.kijiTable
                                .getEntityId(userId.toString()),
                                KijiDataRequest.create(COLUMN_FAMILY_NAME,
                                        COLUMN_NAME));
                if (since.containsColumn(COLUMN_FAMILY_NAME, COLUMN_NAME)){
                    since_id = since.getMostRecentValue(COLUMN_FAMILY_NAME,
                            COLUMN_NAME);
                }
            }catch (IOException ioException){
                ioException.printStackTrace();
                crisisMailer.sendEmailAlert(ioException);
            }
        }
        return since_id;
    }
}
