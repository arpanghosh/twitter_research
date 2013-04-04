package com.edge.twitter_research.relevance_filter;

import org.apache.log4j.Logger;

import com.edge.twitter_research.core.*;

import org.apache.log4j.PropertyConfigurator;
import org.jsoup.Jsoup;

import cmu.arktweetnlp.Tagger;

import java.util.Calendar;
import java.util.List;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;


public class TweetFeatureVectorGenerator {

    private Logger logger;
    private CrisisMailer crisisMailer;
    private static Pattern emoticonMatchPattern;
    private static Pattern mobileSourceMatchPattern;
    private SimpleDateFormat twitterDateFormat;
    private String tweetText;


    private long id;
    private int timeOfDay;
    private int dayOfWeek;
    private int urlLocationInTweet;
    private long retweetCount;
    private int numCharsInTweet;
    private int numEmoticonsInTweet;
    private int hasContributors;
    private int isReply;
    private int isPossiblySensitive;
    private int isQuestion;
    private int isMobileSource;
    private double[] componentFractions;
    private long userFriends;
    private long userFollowers;
    private int userTotalTweets;
    private int userIsVerified;


    public TweetFeatureVectorGenerator(String log4jPropertiesFilePath){
        logger = Logger.getLogger(TweetFeatureVectorGenerator.class);
        PropertyConfigurator.configure(log4jPropertiesFilePath);

        crisisMailer = CrisisMailer.getCrisisMailer();

        twitterDateFormat = new SimpleDateFormat("EEE MM dd HH:mm:ss Z yyyy");

        mobileSourceMatchPattern = Pattern.compile("(\\Qobile\\E|\\QiOS\\E|\\Qapp\\E|\\QApp\\E|\\QPlayStation\\E|" +
                "\\Qokia\\E|\\Qnstag\\E|\\Qnap\\E|\\Qstream\\E|\\Qfon\\E|\\Qnette\\E|\\Qrite\\E|\\Qwicc\\E|\\Qweetlog\\E|" +
                "\\Qterous\\E|\\Qastin\\E)");

        emoticonMatchPattern = Pattern.compile("(\\Q:)\\E|\\Q:D\\E|\\Q:(\\E|\\Q;)\\E|\\Q:-)\\E|\\Q:P\\E|\\Q=)" +
                "\\E|\\Q(:\\E|\\Q;-)\\E|\\Q:/\\E|\\QXD\\E|\\Q=D\\E|\\Q=]\\E|\\QD:\\E|\\Q;D\\E|\\Q:]\\E|\\Q:-(\\E|\\Q=/\\E|" +
                "\\Q:O\\E|\\Q=(\\E|\\Q):\\E|\\Q=P\\E|\\Q:'(\\E|\\Q:|\\E|\\Q:-D\\E|\\Q^_^\\E|\\Q(8\\E|\\Q:-/\\E|\\Q:o)\\E|" +
                "\\Q:o\\E|\\Q:-P\\E|\\Q(;\\E|\\Q;P\\E|\\Qo:\\E|\\Q;]\\E|\\Q:@\\E|\\Q=[\\E|\\Q:\\\\E|\\Q;(\\E|\\Q:[\\E|\\Q8)" +
                "\\E|\\Q;o)\\E|\\Q=\\\\E|\\Q=O\\E|\\Q(=\\E|\\Q[:\\E|\\Q;/\\E|\\Q8D\\E|\\Q:}\\E|\\Q\\m/\\E|\\QO:\\E|\\Q/:\\E|" +
                "\\Q;O\\E|\\Q^-^\\E|\\Q8-)\\E|\\Q=|\\E|\\Q]:\\E|\\QD;\\E|\\Q:o(\\E|\\Q|:\\E|\\Q;-P\\E|\\Q);\\E|\\Q=o\\E|\\Q;-D" +
                "\\E|\\Q:-\\\\E|\\Q(^_^)\\E|\\Q:-O\\E|\\Q:-o\\E|\\QD=\\E|\\Q(^_^;)\\E|\\Q;o\\E|\\Q;-(\\E|\\Q;@\\E|\\QP:\\E|" +
                "\\Q@:\\E|\\Q:-|\\E|\\Q[=\\E|\\Q(^-^)\\E|\\Q[8\\E|\\Q(T_T)\\E|\\Q(-_-)\\E|\\Q(-:\\E|\\Q)=\\E|\\Q:{\\E|\\Q=}" +
                "\\E|\\Q[;\\E|\\Q:?\\E|\\Q8-]\\E|\\Q:*(\\E|\\Qo;\\E|\\QD8\\E|\\Q;}\\E|\\Q;[\\E|\\Q:o/\\E|\\Q:oP\\E|\\Q:-]\\E|" +
                "\\Q:oD\\E|\\Q8/\\E|\\Q8(\\E|\\Qo(^-^)o\\E)");

    }


    public String getFeatureVector(SimpleTweet simpleTweet){
        try{
            tweetText = simpleTweet.getText().toString();
        }catch (NullPointerException nullPointerException){
            logger.warn("Tweet text is null", nullPointerException);
            tweetText = "";
        }

        userFriends = simpleTweet.getUser().getFriendsCount();
        userFollowers = simpleTweet.getUser().getFollowersCount();
        userTotalTweets = simpleTweet.getUser().getStatusesCount();
        userIsVerified = getVerified(simpleTweet);

        id = simpleTweet.getId();

        int[] dateFeatures = getDateFeatures(getDateObject(simpleTweet));
        dayOfWeek = dateFeatures[0];
        timeOfDay = dateFeatures[1];

        componentFractions = getComponentFractions(simpleTweet);

        urlLocationInTweet = getUrlLocationInTweet(simpleTweet);

        retweetCount = simpleTweet.getRetweetCount();

        numCharsInTweet = tweetText.length();

        hasContributors = simpleTweet.getContributors().isEmpty() ? 0 : 1;

        isReply = simpleTweet.getInReplyToScreenName() == null ? 0 : 1;

        isPossiblySensitive = simpleTweet.getIsPossiblySensitive() ? 1 : 0;

        isQuestion = tweetText.contains("?") ? 1 : 0;

        numEmoticonsInTweet = emoticonCount();

        isMobileSource = isMobileSource(simpleTweet);

        return featureVectorToString();
    }

    private int getVerified(SimpleTweet tweet){
        if (tweet.getUser().getVerified())
            return 1;
        return 0;
    }

    private int getUrlLocationInTweet(SimpleTweet simpleTweet){
        if (simpleTweet.getUrlEntities().isEmpty()){
            return 0;
        }

        SimpleURLEntity firstURL = simpleTweet.getUrlEntities().get(0);
        if (firstURL.getStart() == 0)
            return 1;
        else if (firstURL.getEnd() == tweetText.length() - 1)
            return 2;
        else
            return 3;
    }


    private int isMobileSource(SimpleTweet simpleTweet){
        String cleanSource;
        try{
            cleanSource = Jsoup.parse(simpleTweet.getSource().toString())
                            .text().replaceAll("\\s", "");
        }catch (NullPointerException nullPointerException){
            cleanSource = "";
        }

        if ((cleanSource.contains("for") &&
                !cleanSource.contains("forMac")) ||
                (mobileSourceMatchPattern.split(tweetText)).length > 1){
            return 1;
        }
        return 0;
    }


    private int emoticonCount(){
        return emoticonMatchPattern.split(tweetText).length - 1;
    }


    private double[] getComponentFractions(SimpleTweet simpleTweet){
        int urlChars = 0;
        int hashtagChars = 0;
        int mentionChars = 0;
        int mediaChars = 0;

        List<SimpleURLEntity> urls = simpleTweet.getUrlEntities();
        for (SimpleURLEntity url : urls){
            try{
                urlChars += url.getDisplayURL().length();
            }catch (NullPointerException nullPointerException){
                logger.warn("URL entity is null", nullPointerException);
            }
        }

        List<SimpleHashtagEntity> hashtags = simpleTweet.getHashTagEntities();
        for (SimpleHashtagEntity hashtag : hashtags){
            try{
                hashtagChars += hashtag.getText().length() + 1;
            }catch (NullPointerException nullPointerException){
                logger.warn("Hashtag entity is null", nullPointerException);
            }
        }

        List<SimpleUserMentionEntity> mentions = simpleTweet.getUserMentionEntities();
        for (SimpleUserMentionEntity mention : mentions){
            try{
                mentionChars += mention.getScreenName().length() + 1;
            }catch (NullPointerException nullPointerException){
                logger.warn("Mention entity is null", nullPointerException);
            }
        }

        List<SimpleMediaEntity> mediaEntities = simpleTweet.getMediaEntities();
        for(SimpleMediaEntity mediaEntity : mediaEntities){
            try{
                mediaChars += mediaEntity.getMediaURL().length();
            }catch (NullPointerException nullPointerException){
                logger.warn("Media entity is null", nullPointerException);
            }
        }

        double[] componentFractions = new double[5];
        componentFractions[0] = urlChars/tweetText.length();
        componentFractions[1] = hashtagChars/tweetText.length();
        componentFractions[2] = mentionChars/tweetText.length();
        componentFractions[3] = mediaChars/tweetText.length();
        componentFractions[4] = 1.0 - (componentFractions[0] +
                                        componentFractions[1] +
                                        componentFractions[2] +
                                        componentFractions[3]);

        if (componentFractions[4] < 0.0){
            logger.error("Error in calculating component fractions");
            crisisMailer.sendEmailAlert("Component Fractions do not add up to one");
        }

        return componentFractions;
    }


    private Date getDateObject(SimpleTweet simpleTweet){
        try{
            return twitterDateFormat.parse(simpleTweet.getCreatedAt().toString());
        }catch(ParseException parseException){
            logger.error("Date string is not valid", parseException);
            crisisMailer.sendEmailAlert(parseException);
        }catch (NullPointerException nullPointerException){
            logger.warn("createdAt in SimpleTweet is null", nullPointerException);
        }
        return null;
    }


    private int[] getDateFeatures(Date tweetDate){
        int[] dateFeatures = new int[2];

        if (tweetDate != null){
            Calendar c = Calendar.getInstance();
            c.setTime(tweetDate);
            int day = c.get(Calendar.DAY_OF_WEEK);
            int hour = c.get(Calendar.HOUR_OF_DAY);

            if(!(day == Calendar.SATURDAY) && !(day == Calendar.SUNDAY))
                dateFeatures[0] = 1;

            if(hour >= 0  && hour < 3)
                dateFeatures[1] = 0;
            else if(hour > 2 && hour < 6)
                dateFeatures[1] = 1;
            if(hour >5 && hour < 9)
                dateFeatures[1] = 2;
            else if(hour > 8 && hour < 12)
                dateFeatures[1] = 3;
            else if(hour > 11 && hour < 15)
                dateFeatures[1] = 4;
            else if(hour > 14 && hour < 18)
                dateFeatures[1] = 5;
            else if(hour > 17 && hour < 21)
                dateFeatures[1] = 6;
            else if(hour > 20 && hour < 24)
                dateFeatures[1] = 7;
        }
        return dateFeatures;
    }


    private String featureVectorToString(){
        StringBuilder featureVector = new StringBuilder();
        featureVector.append(id);
        featureVector.append("|");
        featureVector.append(userFollowers);
        featureVector.append("|");
        featureVector.append(userFriends);
        featureVector.append("|");
        featureVector.append(userIsVerified);
        featureVector.append("|");
        featureVector.append(userTotalTweets);
        featureVector.append("|");
        featureVector.append(timeOfDay);
        featureVector.append("|");
        featureVector.append(dayOfWeek);
        featureVector.append("|");
        featureVector.append(urlLocationInTweet);
        featureVector.append("|");
        featureVector.append(retweetCount);
        featureVector.append("|");
        featureVector.append(numCharsInTweet);
        featureVector.append("|");
        featureVector.append(numEmoticonsInTweet);
        featureVector.append("|");
        featureVector.append(hasContributors);
        featureVector.append("|");
        featureVector.append(isReply);
        featureVector.append("|");
        featureVector.append(isPossiblySensitive);
        featureVector.append("|");
        featureVector.append(isQuestion);
        featureVector.append("|");
        featureVector.append(isMobileSource);

        for(double componentFraction : componentFractions){
            featureVector.append("|");
            featureVector.append(componentFraction);
        }

        return featureVector.toString();
    }

}

