package com.edge.twitter_research.relevance_filter;

/**
 * Created with IntelliJ IDEA.
 * User: Mo
 * Date: 3/5/13
 * Time: 5:23 PM
 * To change this template use File | Settings | File Templates
 */

import com.edge.twitter_research.core.*;

public class TweetFeatureVector {

    long[] feature_vector;

    public TweetFeatureVector(SimpleTweet simpleTweet){
        feature_vector = new long[17];
        feature_vector[0] = simpleTweet.getId();
        if (!simpleTweet.getContributors().isEmpty())
            feature_vector[1] = 1;
        String date_info = simpleTweet.getCreatedAt().toString();
        String weekday = date_info.substring(14, 17);
        String hour = date_info.substring(25, 27);
        int time = Integer.parseInt(hour);
        int date_data = 0;
        if (((((weekday.equals("Mon")
                || weekday.equals("Tues"))
                || weekday.equals("Wed"))
                ||weekday.equals("Thurs"))
                ||weekday.equals("Fri")))
            date_data = 10;
        else if (weekday.equals("Sat")
                || weekday.equals("Sun"))
            date_data = 0;
        if ((time < 13) && time > 6)
            date_data += 0;
        else if ((time >= 13) && time < 18)
            date_data += 4;
        else
            date_data += 9;
        feature_vector[2] = date_data;
        if (!simpleTweet.getUrlEntities().isEmpty())
            feature_vector[3] = 1;
        //URL position goes here 0 for front, 1 for center, 2 for back

        int[] url_loc = {simpleTweet.getUrlEntities().get(0).getStart(), simpleTweet.getUrlEntities().get(0).getEnd()};
        if(url_loc[0] <= 10)
            feature_vector[4] = 0;
        else if (url_loc[1] >= (simpleTweet.getText().length() - 10))
            feature_vector[4] = 2;
        else
            feature_vector[4] = 1;
        feature_vector[5] = simpleTweet.getHashTagEntities().size();
        feature_vector[6] = simpleTweet.getUserMentionEntities().size();
        if (!simpleTweet.getMediaEntities().isEmpty())
            feature_vector[7] = 1;
        //filter level... New feature, will probably have to update simpleTweet
        if (simpleTweet.getInReplyToStatusId() != null)
            feature_vector[9] = 1;
        //place... A bit problematic. Ignore for now.
        if (simpleTweet.getIsPossiblySensitive())
            feature_vector[11] = 1;
        feature_vector[12] = simpleTweet.getRetweetCount();
        //Source
        feature_vector[14] = simpleTweet.getText().length();
        //Emotes. Generated this giant regex of the Wikipedia list. Not sure if this actually owrks since I don't have enough test data.
        String tweet_text = simpleTweet.getText().toString();
        if(tweet_text.matches("\\Q?:-)\\E(|\\Q:)\\E|\\Q:o)\\E|\\Q:]\\E|\\Q:3\\E|\\Q:c)\\E|\\Q:>\\E|\\Q=]\\E|\\Q8)\\E|\\Q=)\\E|\\Q:}\\E|\\Q:^)\\E|\\Q:?)\\E|\\Q:-D\\E|\\Q:D\\E|\\Q8-D\\E|\\Q8D\\E|\\Qx-D\\E|\\QxD\\E|\\QX-D\\E|\\QXD\\E|\\Q=-D\\E|\\Q=D\\E|\\Q=-3\\E|\\Q=3\\E|\\QB^D\\E|\\Q:-))\\E|\\Q>:[\\E|\\Q:-(\\E|\\Q:( \\E|\\Q:-c\\E|\\Q:c\\E|\\Q:-<\\E|\\Q:?C\\E|\\Q:<\\E|\\Q:-[\\E|\\Q:[\\E|\\Q:{\\E|\\Q:-||\\E|\\Q:@\\E|\\Q:'-(\\E|\\Q:'(\\E|\\Q:'-)\\E|\\Q:')\\E|\\QQQ\\E|\\QD:<\\E|\\QD:\\E|\\QD8\\E|\\QD;\\E|\\QD=\\E|\\QDX\\E|\\Qv.v\\E|\\QD-':\\E|\\Q>:O\\E|\\Q:-O\\E|\\Q:O\\E|\\Q°o°\\E|\\Q°O°\\E|\\Q:O\\E|\\Qo_O\\E|\\Qo_0\\E|\\Qo.O\\E|\\Q8-0\\E|\\Q:*\\E|\\Q:^*\\E|\\Q('}{')\\E|\\Q;-)\\E|\\Q;)\\E|\\Q*-)\\E|\\Q*)\\E|\\Q;-]\\E|\\Q;]\\E|\\Q;D\\E|\\Q;^)\\E|\\Q:-,\\E|\\Q>:P\\E|\\Q:-P\\E|\\Q:P\\E|\\QX-P\\E|\\Qx-p\\E|\\Qxp\\E|\\QXP\\E|\\Q:-p\\E|\\Q:p\\E|\\Q=p\\E|\\Q:-Þ\\E|\\Q:Þ\\E|\\Q:-b\\E|\\Q:b\\E|\\Q>:\\\\E|\\Q>:/\\E|\\Q:-/\\E|\\Q:-.\\E|\\Q:/\\E|\\Q:\\\\E|\\Q=/\\E|\\Q=\\\\E|\\Q:L\\E|\\Q=L\\E|\\Q:S\\E|\\Q>.<\\E|\\Q:-|\\E|\\Q:$\\E|\\Q:-X\\E|\\Q:X\\E|\\Q:-#\\E|\\Q:#\\E|\\QO:-)\\E|\\Q0:-3\\E|\\Q0:3\\E|\\Q0:-)\\E|\\Q0:)\\E|\\Q0;^)\\E|\\Q>:)\\E|\\Q>;)\\E|\\Q>:-)\\E|\\Q}:-)\\E|\\Q}:)\\E|\\Q3:-)\\E|\\Q3:)\\E|\\Qo/\\o\\E|\\Q^5\\E|\\Q>_>\\E|\\Q|;-)\\E|\\Q|-O\\E|\\Q:-&\\E|\\Q:&\\E|\\Q#-)\\E|\\Q%-)\\E|\\Q%)\\E|\\Q:-###..\\E|\\Q:###..\\E|\\Q<:-|\\E|\\Q?_?\\E|\\Q?_??\\E|\\Q\\o/\\E|\\Q*\\0/*\\E|\\Q@}-;-'---\\E|\\Q@>-->--\\E|\\Q~(_8^(I)\\E|\\Q5:-)\\E|\\Q~:-\\\\E|\\Q//0-0\\\\\\E|\\Q*<|:-)\\E|\\Q=:o]\\E|\\Q,:-)\\E|\\Q7:^]\\E|\\Q[ ]<\\E|\\Q<3\\E|\\Q</3\\E)"))
        //Questions
        if(tweet_text.matches(".*?.*"))
            feature_vector[16] = 1;
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
