package com.edge.twitter_research.queries;

import com.edge.twitter_research.core.CompanyData;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

import java.io.IOException;
import java.util.*;


public class CountedWordsSorter
        extends KijiReducer<Text, AvroValue<WordCount>,
        Text, LongWritable>
        implements AvroValueReader {

    private TreeSet<WordCount> wordCounts;



    public Schema getAvroValueReaderSchema() throws IOException {
        return CompanyOccurrenceType.SCHEMA$;
    }


    public void setup(Context context)throws IOException, InterruptedException{
        super.setup(context);

        wordCounts = new TreeSet<WordCount>(new Comparator<WordCount>() {
            @Override
            public int compare(WordCount wordCount1, WordCount wordCount2) {
                return wordCount2.getCount().compareTo(wordCount1.getCount());
            }
        });
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return LongWritable.class;
    }



    @Override
    protected void reduce(Text key, Iterable<AvroValue<WordCount>> values, Context context)
            throws IOException, InterruptedException {

        wordCounts.clear();

        for (AvroValue<WordCount> value : values ){
            WordCount wc = WordCount.newBuilder(value.datum()).build();
            wordCounts.add(wc);
        }

        for (WordCount wc : wordCounts){
            context.write(new Text(wc.getWord().toString()),
                    new LongWritable(wc.getCount()));
        }
    }


}

