package com.edge.twitter_research.topic_detection;


import com.edge.twitter_research.core.CompanyData;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;

import java.io.IOException;

public class CompanyUserListToCSVMapper
        extends KijiMapper<AvroKey<CompanyData>, AvroValue<UserCountList>, LongWritable, Text>
        implements AvroKeyReader, AvroValueReader {

    UserFeatureVectorGenerator userFeatureVectorGenerator;
    String companyName;

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException{
        super.setup(context);

        userFeatureVectorGenerator = new UserFeatureVectorGenerator();

        Configuration conf = context.getConfiguration();
        companyName = conf.get("company.name", "");
    }

    @Override
    public void map(AvroKey<CompanyData> key, AvroValue<UserCountList> value, Context context)
            throws IOException, InterruptedException {

        if (companyName.equals(key.datum().getCompanyName().toString())){
            for (UserCount userCount : UserCountList.newBuilder(value.datum()).build().getUserCountList()){

                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(userCount.getUser().getScreenName());
                stringBuilder.append(",");
                stringBuilder.append(companyName);
                stringBuilder.append(",");
                stringBuilder.append(userCount.getCount());
                stringBuilder.append(",");
                stringBuilder.append(userFeatureVectorGenerator.getCSV(userCount.getUser()));

                context.write(new LongWritable(userCount.getUser().getUserId()),
                                new Text(stringBuilder.toString()));
            }
        }
    }


    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return CompanyData.SCHEMA$;
    }


    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return UserCountList.SCHEMA$;
    }

    @Override
    public Class<?> getOutputValueClass() {
        return Text.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return LongWritable.class;
    }
}
