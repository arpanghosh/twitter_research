package com.edge.twitter_research.queries;


import com.edge.twitter_research.core.CompanyData;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;

import java.io.IOException;
import java.util.*;


public class PerCompanyCSVGenerator
        extends KijiReducer<AvroKey<CompanyData>, AvroValue<CompanyTweetCountInTimeUnit>,
        Text, Text>
        implements AvroValueReader, AvroKeyReader {

    private String granularity;

    private TreeMap<GranularDate, TweetTypes> perTimeCompanyCounts;

    public Schema getAvroKeyReaderSchema() throws IOException {
        return CompanyData.SCHEMA$;
    }

    public Schema getAvroValueReaderSchema() throws IOException {
        return CompanyTweetCountInTimeUnit.SCHEMA$;
    }


    public void setup(Context context)throws IOException, InterruptedException{
        super.setup(context);

        Configuration configuration = context.getConfiguration();
        granularity = configuration.get("granularity", "hourly");
        perTimeCompanyCounts = new TreeMap<GranularDate, TweetTypes>(new Comparator<GranularDate>() {
            @Override
            public int compare(GranularDate granularDate1, GranularDate granularDate2) {

                if (granularity.equals("weekly")){
                    return granularDate1.getWeekOfYear().compareTo(granularDate2.getWeekOfYear());
                }else if (granularity.equals("daily")){
                    return granularDate1.getDayOfYear().compareTo(granularDate2.getDayOfYear());
                }else{
                    Calendar c1 = Calendar.getInstance();
                    Calendar c2 = Calendar.getInstance();

                    c1.set(granularDate1.getYear(),
                            granularDate1.getMonthOfYear(),
                            granularDate1.getDayOfMonth(),
                            granularDate1.getHourOfDay(),
                            0);

                    c2.set(granularDate2.getYear(),
                            granularDate2.getMonthOfYear(),
                            granularDate2.getDayOfMonth(),
                            granularDate2.getHourOfDay(),
                            0);
                    return c1.getTime().compareTo(c2.getTime());
                }
            }
        });

    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return Text.class;
    }



    @Override
    protected void reduce(AvroKey<CompanyData> key, Iterable<AvroValue<CompanyTweetCountInTimeUnit>> values, Context context)
            throws IOException, InterruptedException {


        perTimeCompanyCounts.clear();


        for (AvroValue<CompanyTweetCountInTimeUnit> value : values){
            CompanyTweetCountInTimeUnit companyTweetCountInTimeUnit =
                    CompanyTweetCountInTimeUnit.newBuilder(value.datum()).build();

            if (granularity.equals("weekly")){
                GranularDate granularDate = companyTweetCountInTimeUnit.getGranularDate();
                granularDate.setDayOfYear(-1);
                granularDate.setDayOfMonth(-1);
                granularDate.setDayOfWeek(-1);
                granularDate.setHourOfDay(-1);

                if (perTimeCompanyCounts.containsKey(granularDate)){
                    TweetTypes tweetTypes = perTimeCompanyCounts.get(granularDate);
                    tweetTypes.setRegularCount(tweetTypes.getRegularCount() +
                            companyTweetCountInTimeUnit.getCount().getRegularCount());
                    tweetTypes.setRetweetCount(tweetTypes.getRetweetCount() +
                            companyTweetCountInTimeUnit.getCount().getRetweetCount());
                }else{
                    perTimeCompanyCounts.put(granularDate, companyTweetCountInTimeUnit.getCount());
                }
            }else if (granularity.equals("daily")){
                GranularDate granularDate = companyTweetCountInTimeUnit.getGranularDate();
                granularDate.setHourOfDay(-1);

                if (perTimeCompanyCounts.containsKey(granularDate)){
                    TweetTypes tweetTypes = perTimeCompanyCounts.get(granularDate);
                    tweetTypes.setRegularCount(tweetTypes.getRegularCount() +
                            companyTweetCountInTimeUnit.getCount().getRegularCount());
                    tweetTypes.setRetweetCount(tweetTypes.getRetweetCount() +
                            companyTweetCountInTimeUnit.getCount().getRetweetCount());
                }else{
                    perTimeCompanyCounts.put(granularDate, companyTweetCountInTimeUnit.getCount());
                }
            }else{
                perTimeCompanyCounts.put(companyTweetCountInTimeUnit.getGranularDate(),
                        companyTweetCountInTimeUnit.getCount());
            }
        }


        CompanyData companyData = CompanyData.newBuilder(key.datum()).build();

        for (Map.Entry<GranularDate, TweetTypes> entry : perTimeCompanyCounts.entrySet()){

            StringBuilder features = new StringBuilder();
            features.append(entry.getKey().getYear());
            features.append(",");
            features.append(entry.getKey().getMonthOfYear());
            features.append(",");
            features.append(entry.getKey().getWeekOfYear());
            features.append(",");

            if (granularity.equals("daily")){
                features.append(entry.getKey().getDayOfYear());
                features.append(",");
                features.append(entry.getKey().getDayOfMonth());
                features.append(",");
                features.append(entry.getKey().getDayOfWeek());
                features.append(",");
            }else if (granularity.equals("hourly")){
                features.append(entry.getKey().getDayOfYear());
                features.append(",");
                features.append(entry.getKey().getDayOfMonth());
                features.append(",");
                features.append(entry.getKey().getDayOfWeek());
                features.append(",");
                features.append(entry.getKey().getHourOfDay());
                features.append(",");
            }

            features.append(entry.getValue().getRegularCount());
            features.append(",");
            features.append(entry.getValue().getRetweetCount());

            context.write(new Text(companyData.getCompanyName() + "," + companyData.getCompanyArea()),
                    new Text(features.toString()));
        }
    }
}


