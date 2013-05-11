package com.edge.twitter_research.queries;


import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroValueReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class EventDetector
        extends KijiReducer<Text, AvroValue<GranularDateSortedWordCounts>,
        Text, Text>
        implements AvroValueReader {

    private TreeMap<GranularDate, SortedWordCounts> sortedWordCountsSortedByDate;
    private Cache<String, Double> dictionary;
    private Ticker dayTicker;
    private long day = 90;


    @Override
    public void setup (Context context)
            throws IOException, InterruptedException{
        super.setup(context);

        sortedWordCountsSortedByDate = new TreeMap<GranularDate, SortedWordCounts>(new Comparator<GranularDate>() {
            @Override
            public int compare(GranularDate granularDate1, GranularDate granularDate2) {
                return granularDate1.getDayOfYear().compareTo(granularDate2.getDayOfYear());
            }
        });

        dayTicker = new Ticker() {
            @Override
            public long read() {
                return day;
            }
        };

        dictionary = CacheBuilder.newBuilder()
                .maximumSize(7000)
                .expireAfterAccess(7, TimeUnit.NANOSECONDS)
                .initialCapacity(1000)
                .ticker(dayTicker)
                .build();
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
    public Schema getAvroValueReaderSchema() throws IOException {
        return GranularDateSortedWordCounts.SCHEMA$;
    }



    @Override
    protected void reduce(Text key, Iterable<AvroValue<GranularDateSortedWordCounts>> values, Context context)
            throws IOException, InterruptedException {

        sortedWordCountsSortedByDate.clear();
        day = 90;
        dictionary.invalidateAll();

        for (AvroValue<GranularDateSortedWordCounts> value : values){
            GranularDateSortedWordCounts granularDateSortedWordCounts =
                    GranularDateSortedWordCounts.newBuilder(value.datum()).build();

            if (granularDateSortedWordCounts.getGranularDate().getDayOfYear() >= 90){

                sortedWordCountsSortedByDate.put(granularDateSortedWordCounts.getGranularDate(),
                                                    granularDateSortedWordCounts.getSortedWordCounts());
            }
        }


        for (Map.Entry<GranularDate, SortedWordCounts> entry : sortedWordCountsSortedByDate.entrySet()){
            day = entry.getKey().getDayOfYear();
            double totalWords = getTotalWords(entry.getValue());

            int newWords = 0;
            for (WordCount wordCount : entry.getValue().getSortedWordCounts()){

                if (dictionary.getIfPresent(wordCount.getWord().toString()) != null){
                    dictionary.put(wordCount.getWord().toString(), wordCount.getCount()/totalWords);
                }else{
                    newWords++;
                    dictionary.put(wordCount.getWord().toString(), wordCount.getCount()/totalWords);
                }
            }

            context.write(key, new Text(entry.getKey().getDayOfYear() + ":" + newWords));
        }
    }


    private double getTotalWords(SortedWordCounts sortedWordCounts){
        long total = 0;

        for (WordCount wordCount : sortedWordCounts.getSortedWordCounts()){
            total += wordCount.getCount();
        }
        return (double)total;
    }

}

