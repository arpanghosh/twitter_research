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
import java.util.Locale;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import java.util.Calendar;

public class TweetFeatureVector {

    long[] feature_vector;
    long id;
    int has_contributers;
    int daytime;
    int weekday;
    int has_urls;
    int url_place;
    int num_hashtags;
    int num_mentions;
    int has_media;
    int reply;
    int sensitive;
    long retweet_count;
    int text_size;
    int num_emotes;
    int question;
    int isMobile;

    public TweetFeatureVector(SimpleTweet simpleTweet){
        id = simpleTweet.getId();
        if (!simpleTweet.getContributors().isEmpty())
            has_contributers = 1;
        String date_info = simpleTweet.getCreatedAt().toString();
        Date myDate = get_Date(date_info);
        Calendar c = Calendar.getInstance();
        c.setTime(myDate);
        int day = c.get(Calendar.DAY_OF_WEEK);
        int time = c.get(Calendar.HOUR_OF_DAY);
        if(!(day == Calendar.SATURDAY) && !(day == Calendar.SUNDAY))
            weekday = 1;
        if(time < 13 && time > 5)
            daytime = 1;
        else if(time > 12 && time < 16)
            daytime = 2;
        else if(time > 15 && time < 23)
            daytime = 3;
        else
            daytime = 4;
        if(!simpleTweet.getUrlEntities().isEmpty())
            has_urls = 1;
        num_hashtags = simpleTweet.getHashTagEntities().size();
        num_mentions = simpleTweet.getUserMentionEntities().size();
        if (!simpleTweet.getMediaEntities().isEmpty())
            has_media = 1;
        //filter level... New feature, will probably have to update simpleTweet
        if (simpleTweet.getInReplyToStatusId() != null)
            reply = 1;
        //place... A bit problematic. Ignore for now.
        if (simpleTweet.getIsPossiblySensitive())
            sensitive = 1;
        retweet_count = simpleTweet.getRetweetCount();
        //Source
        text_size = simpleTweet.getText().length();
        //URL position goes here 1 for front, 2 for center, 3 for back
        int len = url_length(simpleTweet);
        int loc = url_loc(simpleTweet);
        int text = text_size - len;
        if(((double)loc)/text < 1/5)
            url_place = 1;
        else if (((double)loc)/text > 1/5)
            url_place = 3;
        else
            url_place = 2;
        //Emotes. Generated this giant regex of the Wikipedia list. Not sure if this actually owrks since I don't have enough test data.
        String tweet_text = simpleTweet.getText().toString();

        Pattern emote_match = Pattern.compile("\\Q:)\\E(|\\Q:D\\E|\\Q:(\\E|\\Q;)\\E|\\Q:-)\\E|\\Q:P\\E|\\Q=)\\E|\\Q(:\\E|" +
                "\\Q;-)\\E|\\Q:/\\E|\\QXD\\E|\\Q=D\\E|\\Q=]\\E|\\QD:\\E|\\Q;D\\E|\\Q:]\\E|\\Q:-(\\E|\\Q=/\\E|\\Q:O\\E|\\Q" +
                "=(\\E|\\Q):\\E|\\Q=P\\E|\\Q:'(\\E|\\Q:|\\E|\\Q:-D\\E|\\Q^_^\\E|\\Q(8\\E|\\Q:-/\\E|\\Q:o)\\E|\\Q:o\\E|\\Q" +
                ":-P\\E|\\Q(;\\E|\\Q;P\\E|\\Qo:\\E|\\Q;]\\E|\\Q:@\\E|\\Q=[\\E|\\Q:\\\\E|\\Q;(\\E|\\Q:[\\E|\\Q8)\\E|\\Q;o)" +
                "\\E|\\Q=\\\\E|\\Q=O\\E|\\Q(=\\E|\\Q[:\\E|\\Q;/\\E|\\Q8D\\E|\\Q:}\\E|\\Q\\m/\\E|\\QO:\\E|\\Q/:\\E|\\Q;O\\E" +
                "|\\Q^-^\\E|\\Q8-)\\E|\\Q=|\\E|\\Q]:\\E|\\QD;\\E|\\Q:o(\\E|\\Q|:\\E|\\Q;-P\\E|\\Q);\\E|\\Q=o\\E|\\Q;-D\\E|" +
                "\\Q:-\\\\E|\\Q(^_^)\\E|\\Q:-O\\E|\\Q:-o\\E|\\QD=\\E|\\Q(^_^;)\\E|\\Q;o\\E|\\Q;-(\\E|\\Q;@\\E|\\QP:\\E|\\Q@:" +
                "\\E|\\Q:-|\\E|\\Q[=\\E|\\Q(^-^)\\E|\\Q[8\\E|\\Q(T_T)\\E|\\Q(-_-)\\E|\\Q(-:\\E|\\Q)=\\E|\\Q:{\\E|\\Q=}\\E" +
                "|\\Q[;\\E|\\Q:?\\E|\\Q8-]\\E|\\Q:*(\\E|\\Qo;\\E|\\QD8\\E|\\Q;}\\E|\\Q;[\\E|\\Q:o/\\E|\\Q:oP\\E|\\Q:-]\\E" +
                "|\\Q:oD\\E|\\Q8/\\E|\\Q8(\\E|\\Qo(^-^)o\\E)");
        String[] no_emotes = emote_match.split(tweet_text);
        num_emotes = no_emotes.length - 1;
        //Questions
        Pattern question_occurances = Pattern.compile(".*?.*");
        String space_stripped = tweet_text.replaceAll("\\s", "");
        String[] questions = question_occurances.split(tweet_text);
        question = 0;
        for(String segment : questions){
            if(!segment.matches(""))
                question++;
        }
        if (question>0)
            question--;

        String source = simpleTweet.getSource().toString();
        String clean_source = Jsoup.parse(source).text();
        clean_source = clean_source.replaceAll("\\s", "");
        if ((clean_source.contains("for") && !clean_source.contains("forMac")) || clean_source.contains("obile")
                                    || clean_source.contains("iOS") || clean_source.contains("app")
                                    || clean_source.contains("App") || clean_source.contains("PlayStation")
                                    || clean_source.contains("Nokia") || clean_source.contains("Insta")
                                    || clean_source.contains("nap") || clean_source.contains("tream"))
            isMobile = 1;


    }

    private static int url_length(SimpleTweet tweet){
        int[] url_loc = {tweet.getUrlEntities().get(0).getStart(), tweet.getUrlEntities().get(0).getEnd()};
        int url_len = url_loc[1] - url_loc[0];
        return url_len;
    }
    private static int url_loc(SimpleTweet tweet){
        int[] url_loc = {tweet.getUrlEntities().get(0).getStart(), tweet.getUrlEntities().get(0).getEnd()};
        return url_loc[0];
    }


    private static Date get_Date(String str){
        try{
        Date myDate = new SimpleDateFormat("EEE dd/MM/yyyy").parse(str);
            return myDate;
        }
        catch(Exception e){
            System.out.println("Input string is not appropriate date");
        }
        return null;
    }

    public long id(){
        return id;
    }

    public String toString(){
        return "" + id + " " + has_contributers + " " + daytime + " " + weekday + " " + has_urls + " " + url_place
                + " " + num_hashtags + " " + num_mentions + " " + has_media + " " + reply + " " + sensitive + " " +
                retweet_count + " " + text_size + " " + num_emotes + " " + question + " " + isMobile;
    }

}

/*id: long
contributors: boolean
created_at: <labelled in 2 dimensions: weekday vs weekend and times of day (morning, afternoon, night)
entities_url: boolean
entities_url_position: <labelled as beginning or end>
entities_hashtags: int
entities_mentions: int
entities_media: boolean
filter_level: <labelled as all the Twitter filter levels>
is_reply: boolean (derived from any of the “in_reply_*” fields)
place: boolean
possibly_sensitive: boolean
retweeted: int
source: <labelled as mobile or desktop> derived from list of client names*/
