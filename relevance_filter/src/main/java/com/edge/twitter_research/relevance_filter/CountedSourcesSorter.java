package com.edge.twitter_research.relevance_filter;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;


public class CountedSourcesSorter
    extends KijiReducer<AvroKey<CharSequence>, AvroValue<SourceCount>, Text, LongWritable>
    implements AvroKeyReader, AvroValueReader {

    private TreeSet<SourceCount> mTopSources;
    private final int mNumberOfTopSources = 200;


    @Override
    public Class<?> getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return SourceCount.SCHEMA$;
    }


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        mTopSources = new TreeSet<SourceCount>(new Comparator<SourceCount>() {
            @Override
            public int compare(SourceCount sourceCount1, SourceCount sourceCount2) {
                if (sourceCount1.getCount().compareTo(sourceCount2.getCount()) == 0) {
                    return sourceCount1.getSource().toString()
                            .compareTo(sourceCount2.getSource().toString());
                } else {
                    return sourceCount1.getCount().compareTo(sourceCount2.getCount());
                }
            }
        });
    }




    @Override
    protected void reduce(AvroKey<CharSequence> key, Iterable<AvroValue<SourceCount>> values, Context context)
            throws IOException, InterruptedException {

        // We are reusing objects, so we should make sure they are cleared for each new key.
        mTopSources.clear();

        for (AvroValue<SourceCount> value : values) {
            // Remove AvroValue wrapper.
            SourceCount currentSourceCount = SourceCount.newBuilder(value.datum()).build();

            mTopSources.add(currentSourceCount);
            // If we now have too many elements, remove the element with the smallest count.

            if (mTopSources.size() > mNumberOfTopSources) {
                mTopSources.pollFirst();
            }

        }

        for (SourceCount sourceCount : mTopSources)
            context.write(new Text(sourceCount.getSource().toString()),
                    new LongWritable(sourceCount.getCount()));
    }

}
