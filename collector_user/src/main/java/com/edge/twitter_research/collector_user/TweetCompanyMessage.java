package com.edge.twitter_research.collector_user;


import com.edge.twitter_research.core.CompanyData;
import twitter4j.Status;

public class TweetCompanyMessage {
    CompanyData companyData;
    Status tweet;

    public TweetCompanyMessage(Status tweet, CompanyData companyData){
        this.tweet = tweet;
        this.companyData = companyData;
    }


}
