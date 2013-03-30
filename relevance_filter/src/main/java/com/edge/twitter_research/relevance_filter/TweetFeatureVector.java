package com.edge.twitter_research.relevance_filter;

/**
 * Created with IntelliJ IDEA.
 * User: Mo
 * Date: 3/5/13
 * Time: 5:23 PM
 * To change this template use File | Settings | File Templates
 */

import com.edge.twitter_research.core.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import java.util.Calendar;
import java.util.List;

public class TweetFeatureVector {

    private static Pattern emote_match = Pattern.compile("(\\Q:)\\E|\\Q:D\\E|\\Q:(\\E|\\Q;)\\E|\\Q:-)\\E|\\Q:P\\E|\\Q=)" +
            "\\E|\\Q(:\\E|\\Q;-)\\E|\\Q:/\\E|\\QXD\\E|\\Q=D\\E|\\Q=]\\E|\\QD:\\E|\\Q;D\\E|\\Q:]\\E|\\Q:-(\\E|\\Q=/\\E|" +
            "\\Q:O\\E|\\Q=(\\E|\\Q):\\E|\\Q=P\\E|\\Q:'(\\E|\\Q:|\\E|\\Q:-D\\E|\\Q^_^\\E|\\Q(8\\E|\\Q:-/\\E|\\Q:o)\\E|" +
            "\\Q:o\\E|\\Q:-P\\E|\\Q(;\\E|\\Q;P\\E|\\Qo:\\E|\\Q;]\\E|\\Q:@\\E|\\Q=[\\E|\\Q:\\\\E|\\Q;(\\E|\\Q:[\\E|\\Q8)" +
            "\\E|\\Q;o)\\E|\\Q=\\\\E|\\Q=O\\E|\\Q(=\\E|\\Q[:\\E|\\Q;/\\E|\\Q8D\\E|\\Q:}\\E|\\Q\\m/\\E|\\QO:\\E|\\Q/:\\E|" +
            "\\Q;O\\E|\\Q^-^\\E|\\Q8-)\\E|\\Q=|\\E|\\Q]:\\E|\\QD;\\E|\\Q:o(\\E|\\Q|:\\E|\\Q;-P\\E|\\Q);\\E|\\Q=o\\E|\\Q;-D" +
            "\\E|\\Q:-\\\\E|\\Q(^_^)\\E|\\Q:-O\\E|\\Q:-o\\E|\\QD=\\E|\\Q(^_^;)\\E|\\Q;o\\E|\\Q;-(\\E|\\Q;@\\E|\\QP:\\E|" +
            "\\Q@:\\E|\\Q:-|\\E|\\Q[=\\E|\\Q(^-^)\\E|\\Q[8\\E|\\Q(T_T)\\E|\\Q(-_-)\\E|\\Q(-:\\E|\\Q)=\\E|\\Q:{\\E|\\Q=}" +
            "\\E|\\Q[;\\E|\\Q:?\\E|\\Q8-]\\E|\\Q:*(\\E|\\Qo;\\E|\\QD8\\E|\\Q;}\\E|\\Q;[\\E|\\Q:o/\\E|\\Q:oP\\E|\\Q:-]\\E|" +
            "\\Q:oD\\E|\\Q8/\\E|\\Q8(\\E|\\Qo(^-^)o\\E)");
    private static Pattern question_occurances = Pattern.compile(".*?.*");
    private static Pattern mobileFinder = Pattern.compile("(\\Qobile\\E|\\QiOS\\E|\\Qapp\\E|\\QApp\\E|\\QPlayStation\\E|" +
            "\\Qokia\\E|\\Qnstag\\E|\\Qnap\\E|\\Qstream\\E|\\Qfon\\E|\\Qnette\\E|\\Qrite\\E|\\Qwicc\\E|\\Qweetlog\\E|" +
            "\\Qterous\\E|\\Qastin\\E)");
    private long id;
    private int daytime;
    private int weekday;
    private int url_place;
    private long retweet_count;
    private int text_size;
    private int num_emotes;
    private int[] t_or_f;
    private int isMobile;
    private double[] fractions;

    public TweetFeatureVector(SimpleTweet simpleTweet){
        id = simpleTweet.getId();
        int[] date = date_info(get_Date(simpleTweet.getCreatedAt().toString()));
        weekday = date[0];
        daytime = date[1];
        fractions = fractions(simpleTweet);
        //filter level... New feature, will probably have to update simpleTweet
        //place... A bit problematic. Ignore for now.
        retweet_count = simpleTweet.getRetweetCount();
        String tweet_text = simpleTweet.getText().toString();
        t_or_f = booleans(simpleTweet, tweet_text);
        text_size = tweet_text.length();
        url_place = url_val(simpleTweet.getUrlEntities().get(0), text_size);
        //Emotes. Generated this giant regex of the Wikipedia list. Not sure if this actually owrks since I don't have enough test data.
        num_emotes = emoteCount(tweet_text);
        //Questions
        isMobile = mobile(simpleTweet.getSource().toString());


    }

    private static int url_val(SimpleURLEntity url, int len){
        int[] loc = url_loc(url);
        int length = len - loc[1] + loc[0] + 1;
        if(loc[0] == 0)
            return 1;
        else if(loc[0] == length - 1)
            return 2;
        return 0;
    }

    private static int[] booleans(SimpleTweet tweet, String str){
        int[] values = new int[4];
        if (!tweet.getContributors().isEmpty())
            values[0] = 1;
        if (tweet.getInReplyToStatusId() != null)
            values[1] = 1;
        if (tweet.getIsPossiblySensitive())
            values[2] = 1;
        if(str.contains("?"))
            values[3] = 1;
        return values;
    }

    private static int mobile(String str){
        String clean_source = Jsoup.parse(str).text();
        clean_source = clean_source.replaceAll("\\s", "");
        if ((clean_source.contains("for") && !clean_source.contains("forMac")) || (mobileFinder.split(str).length > 1))
            return 0;
        return 1;
    }

    private static int emoteCount(String str){
        return emote_match.split(str).length - 1;
    }

    private static double[] fractions(SimpleTweet tweet){
        double urltot = 0;
        double hashtot = 0;
        double menttot = 0;
        double medtot = 0;
        List<SimpleURLEntity> urls = tweet.getUrlEntities();
        for (SimpleURLEntity url : urls)
            urltot += (url_loc(url)[1] - url_loc(url)[1] + 1);
        List<SimpleHashtagEntity> hashes = tweet.getHashTagEntities();
        for (SimpleHashtagEntity hash : hashes)
            hashtot += hash.getText().length() + 1;
        List<SimpleUserMentionEntity> mentions = tweet.getUserMentionEntities();
        for (SimpleUserMentionEntity mention : mentions)
            menttot += mention.getScreenName().length() + 1;
        List<SimpleMediaEntity> media = tweet.getMediaEntities();
        for(SimpleMediaEntity medium : media)
            medtot += medium.getMediaURL().length();
        urltot -= medtot;
        int len = tweet.getText().length();
        double[] parts = {urltot/len, hashtot/len, menttot/len, medtot/len};
        return parts;
    }

    private static int[] url_loc(SimpleURLEntity url){
        int[] url_loc = {url.getStart(), url.getEnd()};
        return url_loc;
    }


    private static Date get_Date(String str){
        try{
        Date myDate = new SimpleDateFormat("EEE dd MM KK:mm:ss ZZZZZ yyyy").parse(str);
            return myDate;
        }
        catch(Exception e){
            System.out.println("Input string is not appropriate date");
        }
        return null;
    }

    private static int[] date_info(Date myDate){
        int[] info = new int[2];
        Calendar c = Calendar.getInstance();
        c.setTime(myDate);
        int day = c.get(Calendar.DAY_OF_WEEK);
        int time = c.get(Calendar.HOUR_OF_DAY);
        if(!(day == Calendar.SATURDAY) && !(day == Calendar.SUNDAY))
            info[0] = 1;
        if(time < 13 && time > 5)
            info[1] = 1;
        else if(time > 12 && time < 16)
            info[1] = 2;
        else if(time > 15 && time < 23)
            info[1] = 3;
        else
            info[1] = 4;
        return info;
    }

    public long id(){
        return id;
    }

    public String toString(){
        StringBuilder featureVector = new StringBuilder();
        featureVector.append(id);
        featureVector.append("|");
        featureVector.append(daytime);
        featureVector.append("|");
        featureVector.append(weekday);
        featureVector.append("|");
        featureVector.append(url_place);
        featureVector.append("|");
        featureVector.append(retweet_count);
        featureVector.append("|");
        featureVector.append(text_size);
        featureVector.append("|");
        featureVector.append(isMobile);
        for(int i = 0; i < t_or_f.length; i++)

            featureVector.append("|" + t_or_f[i]);
        for(int i = 0; i < fractions.length; i++)
            featureVector.append("|" + fractions[i]);
        return fractions.toString();
    }

}

