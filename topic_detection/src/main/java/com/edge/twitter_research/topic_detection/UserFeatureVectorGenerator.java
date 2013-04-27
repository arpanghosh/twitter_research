package com.edge.twitter_research.topic_detection;


import com.edge.twitter_research.core.*;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

    private final int NUM_FEATURES = 6;

    private SimpleDateFormat twitterDateFormat;
    private double[] features;
    private Vector vector;

    int isUserVerified;
    int yearOfAccountCreation;
    int friendsCount;
    int followersCount;
    int statusesCount;
    int listedCount;


    public UserFeatureVectorGenerator() throws IOException {

        twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
        features = new double[NUM_FEATURES];
        vector = new SequentialAccessSparseVector(NUM_FEATURES);
    }


    public Vector getFeatureVector(SimpleUser simpleUser){
        generateFeatureVector(simpleUser);

        features[0] = isUserVerified;
        features[1] = yearOfAccountCreation;
        features[2] = friendsCount;
        features[3] = followersCount;
        features[4] = statusesCount;
        features[5] = listedCount;

        vector.assign(features);

        return vector;
    }


    public String getCSV(SimpleUser simpleUser){
        generateFeatureVector(simpleUser);

        return featureVectorToString();
    }


    public String getCSV(User user){
        generateFeatureVector(user);

        return featureVectorToString();
    }



    private void generateFeatureVector(SimpleUser simpleUser){

        isUserVerified = simpleUser.getVerified()? 1:0;

        yearOfAccountCreation =
                getYearOfAccountCreation(getDateObject(simpleUser.getCreatedAt().toString())).year;

        friendsCount = simpleUser.getFriendsCount();

        followersCount = simpleUser.getFollowersCount();

        statusesCount = simpleUser.getStatusesCount();

        listedCount = simpleUser.getListedCount();
    }


    private void generateFeatureVector(User user){

        isUserVerified = user.getIsVerified()? 1:0;

        yearOfAccountCreation =
                getYearOfAccountCreation(getDateObject(user.getCreatedAt().toString())).year;

        friendsCount = user.getNumFriends();

        followersCount = user.getNumFollowers();

        statusesCount = user.getNumStatuses();

        listedCount = user.getNumListed();
    }


    private Date getDateObject(String dateString){
        try{
            return twitterDateFormat.parse(dateString);
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
        featureVector.append(isUserVerified);
        featureVector.append(",");
        featureVector.append(yearOfAccountCreation);
        featureVector.append(",");
        featureVector.append(friendsCount);
        featureVector.append(",");
        featureVector.append(followersCount);
        featureVector.append(",");
        featureVector.append(statusesCount);
        featureVector.append(",");
        featureVector.append(listedCount);

        return featureVector.toString();
    }

}


