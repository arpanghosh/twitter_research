package com.edge.twitter_research.topic_detection;


import com.edge.twitter_research.core.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class UserFeatureVectorGenerator {

    private enum Year{
        YEAR_2006(2006),
        YEAR_2007(2007),
        YEAR_2008(2008),
        YEAR_2009(2009),
        YEAR_2010(2010),
        YEAR_2011(2011),
        YEAR_2012(2012),
        YEAR_2013(2013);

        public final int year;
        Year(int year){
            this.year = year;
        }
    }


    private SimpleDateFormat twitterDateFormat;

    private int verified;
    private Year yearOfAccountCreation;
    private int friendsCount;
    private int followersCount;
    private int listedCount;
    private int statusesCount;


    public UserFeatureVectorGenerator() throws IOException {
        twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
    }


    public String getFeatureVector(SimpleUser simpleUser){
        generateFeatureVector(simpleUser);
        return featureVectorToString();
    }



    private void generateFeatureVector(SimpleUser simpleUser){

        verified = simpleUser.getVerified()? 1:0;
        yearOfAccountCreation = getYearOfAccountCreation(getDateObject(simpleUser));
        friendsCount = simpleUser.getFriendsCount();
        followersCount = simpleUser.getFollowersCount();
        statusesCount = simpleUser.getStatusesCount();
        listedCount = simpleUser.getListedCount();
    }


    private Date getDateObject(SimpleUser simpleUser){
        try{
            return twitterDateFormat.parse(simpleUser.getCreatedAt().toString());
        }catch(ParseException parseException){
            parseException.printStackTrace();
        }catch (NullPointerException nullPointerException){
            nullPointerException.printStackTrace();
        }
        return null;
    }


    private Year getYearOfAccountCreation(Date accountCreationDate){

        if (accountCreationDate != null){
            Calendar c = Calendar.getInstance();
            c.setTime(accountCreationDate);
            int year = c.get(Calendar.YEAR);

            switch (year){
                case 2006: return Year.YEAR_2006;
                case 2007: return Year.YEAR_2007;
                case 2008: return Year.YEAR_2008;
                case 2009: return Year.YEAR_2009;
                case 2010: return Year.YEAR_2010;
                case 2011: return Year.YEAR_2011;
                case 2012: return Year.YEAR_2012;
                case 2013: return Year.YEAR_2013;
                default: return Year.YEAR_2013;
            }
        }
        return Year.YEAR_2013;
    }


    private String featureVectorToString(){
        StringBuilder featureVector = new StringBuilder();
        featureVector.append(verified);
        featureVector.append("|");
        featureVector.append(yearOfAccountCreation.year);
        featureVector.append("|");
        featureVector.append(friendsCount);
        featureVector.append("|");
        featureVector.append(followersCount);
        featureVector.append("|");
        featureVector.append(listedCount);
        featureVector.append("|");
        featureVector.append(statusesCount);

        return featureVector.toString();
    }

}


