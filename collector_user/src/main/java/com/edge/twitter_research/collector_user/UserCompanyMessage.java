package com.edge.twitter_research.collector_user;


import com.edge.twitter_research.core.CompanyData;

public class UserCompanyMessage {
    long userID;
    CompanyData companyData;

    public UserCompanyMessage(long userID, String company, String companyArea){
        this.userID = userID;
        this.companyData = new CompanyData(company, companyArea);
    }
}
