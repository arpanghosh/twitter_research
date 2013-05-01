package com.edge.twitter_research.queries;


import com.edge.twitter_research.core.CompanyData;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

import java.io.IOException;

public class CountedWordsMapper
        extends KijiMapper<Text, LongWritable,
        Text, AvroValue<WordCount>>
        implements  AvroValueWriter {

    private Text toyota;
    private WordCount wordCount;

    @Override
    public Class<?> getOutputValueClass() {
        return AvroValue.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }


    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return WordCount.SCHEMA$;
    }


    @Override
    public void setup(Context context)
            throws IOException, InterruptedException{
        super.setup(context);

        toyota = new Text("toyota");

        wordCount = new WordCount();
    }


    @Override
    public void map(Text key, LongWritable value, Context context)
            throws IOException, InterruptedException {

        wordCount.setWord(key.toString());
        wordCount.setCount(value.get());

        context.write(toyota, new AvroValue<WordCount>(wordCount));
    }
}
