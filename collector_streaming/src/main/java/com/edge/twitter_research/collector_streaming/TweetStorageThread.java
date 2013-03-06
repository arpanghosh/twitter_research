package com.edge.twitter_research.collector_streaming;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiTable;

import twitter4j.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import  com.edge.twitter_research.core.*;

public class TweetStorageThread extends Thread {

    private static final String COLUMN_FAMILY_NAME = "tweet_object";
    private static final String COLUMN_NAME = "tweet";

    private LinkedBlockingQueue<Status> inputQueue;
    private KijiConnection kijiConnection;
    private CrisisMailer crisisMailer;

    public TweetStorageThread(LinkedBlockingQueue<Status> inputQueue,
                              String tableLayoutPath,
                              String tableName){
        this.inputQueue = inputQueue;
        this.kijiConnection = new KijiConnection(tableLayoutPath, tableName);
        this.crisisMailer = CrisisMailer.getCrisisMailer();
    }

    public void run(){
        Status tweet;
        while(true){
            try{
                tweet = inputQueue.take();
                storeTweet(tweet);
            }catch (InterruptedException interruptedException){
                interruptedException.printStackTrace();
                continue;
            }
        }
    }


    private void storeTweet(Status tweet){
        KijiTable kijiTable = kijiConnection.kijiTable;
        if (kijiTable != null){
            try{
                KijiTableWriter kijiTableWriter =
                        kijiTable.openTableWriter();
                EntityId tweetId =
                        kijiTable.getEntityId(String.valueOf(tweet.getId()));
                kijiTableWriter.put(tweetId,
                                    COLUMN_FAMILY_NAME,
                                    COLUMN_NAME,
                                    System.currentTimeMillis(),
                                    generateSimpleTweet(tweet));
                System.out.println("Tweet Stored");
            }catch (IOException ioException){
                ioException.printStackTrace();
                crisisMailer.sendEmailAlert(ioException);
            }
        }
    }


    private SimpleTweet generateSimpleTweet(Status status){
        SimpleTweet simpleTweet = new SimpleTweet();

        ArrayList<Long> contributors = new ArrayList<Long>();
        for (long contributor : status.getContributors())
            contributors.add(contributor);
        simpleTweet.setContributors(contributors);

        simpleTweet.setCreatedAt(status.getCreatedAt().toString());
        simpleTweet.setCurrentUserRetweetId(status.getCurrentUserRetweetId());
        simpleTweet.setId(status.getId());
        simpleTweet.setInReplyToScreenName(status.getInReplyToScreenName());
        simpleTweet.setInReplyToStatusId(status.getInReplyToStatusId());
        simpleTweet.setInReplyToUserId(status.getInReplyToUserId());
        simpleTweet.setRetweetCount(status.getRetweetCount());
        simpleTweet.setSource(status.getSource());
        simpleTweet.setText(status.getText());
        simpleTweet.setUserId(status.getUser().getId());
        simpleTweet.setIsFavorited(status.isFavorited());
        simpleTweet.setIsPossiblySensitive(status.isPossiblySensitive());
        simpleTweet.setIsRetweet(status.isRetweet());
        simpleTweet.setIsRetweetedByMe(status.isRetweetedByMe());
        simpleTweet.setIsTruncated(status.isTruncated());

        ArrayList<SimpleHashtagEntity> hashtagEntities =
                new ArrayList<SimpleHashtagEntity>();
        for (HashtagEntity hashtagEntity : status.getHashtagEntities()){
            SimpleHashtagEntity simpleHashtagEntity = new SimpleHashtagEntity();
            simpleHashtagEntity.setEnd(hashtagEntity.getEnd());
            simpleHashtagEntity.setStart(hashtagEntity.getStart());
            simpleHashtagEntity.setText(hashtagEntity.getText());
            hashtagEntities.add(simpleHashtagEntity);
        }
        simpleTweet.setHashTagEntities(hashtagEntities);

        ArrayList<SimpleURLEntity> urlEntities =
                new ArrayList<SimpleURLEntity>();
        for (URLEntity urlEntity : status.getURLEntities()){
            SimpleURLEntity simpleURLEntity = new SimpleURLEntity();
            simpleURLEntity.setEnd(urlEntity.getEnd());
            simpleURLEntity.setStart(urlEntity.getStart());
            simpleURLEntity.setURL(urlEntity.getURL());
            simpleURLEntity.setDisplayURL(urlEntity.getDisplayURL());
            simpleURLEntity.setExpandedURL(urlEntity.getExpandedURL());
            urlEntities.add(simpleURLEntity);
        }
        simpleTweet.setUrlEntities(urlEntities);

        ArrayList<SimpleUserMentionEntity> userMentionEntities =
                new ArrayList<SimpleUserMentionEntity>();
        for (UserMentionEntity userMentionEntity : status.getUserMentionEntities()){
            SimpleUserMentionEntity simpleUserMentionEntity = new SimpleUserMentionEntity();
            simpleUserMentionEntity.setEnd(userMentionEntity.getEnd());
            simpleUserMentionEntity.setStart(userMentionEntity.getStart());
            simpleUserMentionEntity.setId(userMentionEntity.getId());
            simpleUserMentionEntity.setName(userMentionEntity.getName());
            simpleUserMentionEntity.setScreenName(userMentionEntity.getScreenName());
            userMentionEntities.add(simpleUserMentionEntity);
        }
        simpleTweet.setUserMentionEntities(userMentionEntities);

        ArrayList<SimpleMediaEntity> mediaEntities =
                new ArrayList<SimpleMediaEntity>();
        for (MediaEntity mediaEntity : status.getMediaEntities()){
            SimpleMediaEntity simpleMediaEntity = new SimpleMediaEntity();
            simpleMediaEntity.setId(mediaEntity.getId());
            simpleMediaEntity.setType(mediaEntity.getType());
            simpleMediaEntity.setMediaURL(mediaEntity.getMediaURL());
            simpleMediaEntity.setMediaURLHttps(mediaEntity.getMediaURLHttps());
            mediaEntities.add(simpleMediaEntity);
        }
        simpleTweet.setMediaEntities(mediaEntities);

        return simpleTweet;
    }
}
