package com.edge.twitter_research.relevance_filter;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

public class TopEmoticonsReducer
    extends KijiReducer<AvroKey<CharSequence>, AvroValue<EmoticonCount>, CharSequence, LongWritable>
    implements AvroValueReader, AvroKeyReader {

    private TreeSet<EmoticonCount> mTopEmoticons;
    private final int mNumberOfTopEmoticons = 100;


    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return EmoticonCount.SCHEMA$;
    }

    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return Schema.create(Schema.Type.STRING);
    }


    @Override
    public Class<?> getOutputKeyClass() {
        return CharSequence.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return LongWritable.class;
    }


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        // This TreeSet will keep track of the "largest" EmoticonCount objects seen so far. Two EmoticonCount
        // objects, emoticon1 and emoticon2, can be compared and the object with the largest value in the field
        // count will the declared the largest object.
        mTopEmoticons = new TreeSet<EmoticonCount>(new Comparator<EmoticonCount>() {
            @Override
            public int compare(EmoticonCount emoticonCount1, EmoticonCount emoticonCount2) {
                if (emoticonCount1.getCount().compareTo(emoticonCount2.getCount()) == 0) {
                    return emoticonCount1.getEmoticon().toString()
                            .compareTo(emoticonCount2.getEmoticon().toString());
                } else {
                    return emoticonCount1.getCount().compareTo(emoticonCount2.getCount());
                }
            }
        });
    }


    @Override
    protected void reduce(AvroKey<CharSequence> key, Iterable<AvroValue<EmoticonCount>> values, Context context)
    throws IOException, InterruptedException {
        // We are reusing objects, so we should make sure they are cleared for each new key.
        mTopEmoticons.clear();

        for (AvroValue<EmoticonCount> value : values) {
            // Remove AvroValue wrapper.
            EmoticonCount currentEmoticonCount = EmoticonCount.newBuilder(value.datum()).build();

            mTopEmoticons.add(currentEmoticonCount);
            // If we now have too many elements, remove the element with the smallest count.
            if (mTopEmoticons.size() > mNumberOfTopEmoticons) {
                mTopEmoticons.pollFirst();
            }
        }

        for (EmoticonCount emoticonCount : mTopEmoticons){
            context.write(emoticonCount.getEmoticon(), new LongWritable(emoticonCount.getCount()));
        }
    }

}
