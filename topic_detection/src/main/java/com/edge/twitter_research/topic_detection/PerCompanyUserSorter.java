package com.edge.twitter_research.topic_detection;

import com.edge.twitter_research.core.CompanyData;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

import static ch.lambdaj.Lambda.select;
import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.theInstance;


import java.io.IOException;
import java.util.*;


public class PerCompanyUserSorter
        extends KijiReducer<AvroKey<CompanyData>, AvroValue<UserCount>, AvroKey<CompanyData>, AvroValue<UserCountList>>
        implements AvroKeyReader, AvroValueReader, AvroKeyWriter, AvroValueWriter {

    private ArrayList<UserCount> sortedUsers;
    private UserCountList sortedUserCountList;
    private int threshold = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);

        sortedUsers = new ArrayList<UserCount>();
        sortedUserCountList = new UserCountList();
        Configuration conf = context.getConfiguration();
        threshold = conf.getInt("threshold", 10);
    }

    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return CompanyData.SCHEMA$;
    }

    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return UserCount.SCHEMA$;
    }


    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        return CompanyData.SCHEMA$;
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return UserCountList.SCHEMA$;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return AvroKey.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return AvroValue.class;
    }


    @Override
    protected void reduce(AvroKey<CompanyData> key, Iterable<AvroValue<UserCount>> values, Context context)
            throws IOException, InterruptedException {

        sortedUsers.clear();

        for(AvroValue<UserCount> value: values){
            sortedUsers.add(UserCount.newBuilder(value.datum()).build());
        }

        Collections.sort(sortedUsers, new Comparator<UserCount>() {
            @Override
            public int compare(UserCount userCount1, UserCount userCount2) {
                return userCount2.getCount().compareTo(userCount1.getCount());
            }
        });

        int filter = (int)(sortedUsers.get(0).getCount() * threshold/100.0);

        sortedUserCountList.setUserCountList(select(sortedUsers, having(on(UserCount.class).getCount(),
                greaterThan(filter))));

        context.write(key,
                new AvroValue<UserCountList>(sortedUserCountList));
    }

}
