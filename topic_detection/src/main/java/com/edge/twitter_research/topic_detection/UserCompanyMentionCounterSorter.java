package com.edge.twitter_research.topic_detection;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;

public class UserCompanyMentionCounterSorter
        extends KijiReducer<AvroKey<UserCompany>, AvroValue<UserVersion>, AvroKey<UserCompany>, AvroValue<UserCount>>
        implements AvroKeyReader, AvroValueReader, AvroKeyWriter, AvroValueWriter {

    private TreeSet<UserVersion> userVersions;
    private UserCount userCount;

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);

        userVersions = new TreeSet<UserVersion>(new Comparator<UserVersion>() {
            @Override
            public int compare(UserVersion userVersion1, UserVersion userVersion2) {
                return userVersion2.getVersion().compareTo(userVersion1.getVersion());
            }
        });
        userCount = new UserCount();
    }

    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return UserCompany.SCHEMA$;
    }

    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return UserVersion.SCHEMA$;
    }

    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        return UserCompany.SCHEMA$;
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return UserCount.SCHEMA$;
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
    protected void reduce(AvroKey<UserCompany> key, Iterable<AvroValue<UserVersion>> values, Context context)
            throws IOException, InterruptedException {

        userVersions.clear();

        for (AvroValue<UserVersion> value : values) {
            userVersions.add(UserVersion.newBuilder(value.datum()).build());
        }

        userCount.setCount(userVersions.size());
        userCount.setUser(userVersions.pollFirst().getUser());

        context.write(key, new AvroValue<UserCount>(userCount));
    }

}
