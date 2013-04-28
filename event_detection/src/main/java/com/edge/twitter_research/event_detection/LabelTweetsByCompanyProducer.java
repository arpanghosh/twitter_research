package com.edge.twitter_research.event_detection;

import com.edge.twitter_research.core.CompanyData;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import org.apache.hadoop.conf.Configuration;
import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

import java.io.IOException;
import java.util.regex.Matcher;

public class LabelTweetsByCompanyProducer
        extends KijiProducer{

    private double threshold;

    @Override
    public void setup(KijiContext context)
        throws IOException{
        super.setup(context);

        Configuration conf = getConf();
        threshold = conf.getFloat("sampling.rate", 100)/100.0;

    }


    @Override
    public KijiDataRequest getDataRequest() {
        return KijiDataRequest.create(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                        GlobalConstants.TWEET_COLUMN_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
        // This is the output column of the kiji table that we write to.
        return (GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME + ":" +
                GlobalConstants.COMPANY_DATA_COLUMN_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
        if (Math.random() < threshold){

            SimpleTweet tweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                    GlobalConstants.TWEET_COLUMN_NAME);
            String tweetText = tweet.getText().toString().toLowerCase();

            for (Constants.Company company : Constants.Company.values()){
                Matcher matcher =
                        company.patternMatcher.reset(tweetText);

                if (matcher.find()){
                    context.put(new CompanyData(company.name, company.area.name));
                }
            }
        }

    }


}

