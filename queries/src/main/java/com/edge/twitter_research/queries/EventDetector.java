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
import java.lang.management.MemoryType;
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
                .maximumSize(2000)
                .expireAfterAccess(3, TimeUnit.NANOSECONDS)
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
            double newWordsLogProbabilities = 0;


            for (WordCount wordCount : entry.getValue().getSortedWordCounts()){

                Double wordProportion = dictionary.getIfPresent(wordCount.getWord().toString());
                double currentWordProportion = wordCount.getCount();

                if (wordProportion != null){

                    newWordsLogProbabilities += (currentWordProportion - wordProportion)/totalWords;

                    double newWordProportion = ((wordProportion * (day - 90)) +
                            currentWordProportion)/(day - 90 + 1);
                    dictionary.put(wordCount.getWord().toString(), newWordProportion);
                }else{
                    newWords++;
                    newWordsLogProbabilities += currentWordProportion/totalWords;

                    dictionary.put(wordCount.getWord().toString(), currentWordProportion);
                }
            }


            if (newWordsLogProbabilities > 0.5 && day != 90){
                StringBuilder stringBuilder = new StringBuilder(500);
                for (WordCount wordCount : entry.getValue().getSortedWordCounts().subList(0, Math.min(100, entry.getValue().getSortedWordCounts().size()))){
                    stringBuilder.append(wordCount.getWord());
                    stringBuilder.append(" ");
                }

                context.write(key, new Text(entry.getKey().getDayOfYear() + ":" + newWordsLogProbabilities + ":" + stringBuilder.toString() + "\n\n\n"));
            }
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

