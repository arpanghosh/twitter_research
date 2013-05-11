package com.edge.twitter_research.queries;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

import java.io.IOException;
import java.util.*;


public class PerCompanyTimeWordCounter
        extends KijiReducer<AvroKey<CompanyGranularDate>, AvroValue<WordCount>,
        AvroKey<CompanyGranularDate>, AvroValue<SortedWordCounts>>
        implements AvroKeyReader, AvroValueReader, AvroKeyWriter, AvroValueWriter{

    private HashMap<String, Long> wordCounts;
    private ArrayList<WordCount> sortedWordCountsList;
    private SortedWordCounts sortedWordCountsValue;

    private StopWords stopWords;


    @Override
    public void setup (Context context)
        throws IOException, InterruptedException{
        super.setup(context);

        wordCounts = new HashMap<String, Long>();
        sortedWordCountsList = new ArrayList<WordCount>();
        sortedWordCountsValue = new SortedWordCounts();

        stopWords = new StopWords();
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
    public Schema getAvroKeyWriterSchema() throws IOException {
        return CompanyGranularDate.SCHEMA$;
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return SortedWordCounts.SCHEMA$;
    }

    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return CompanyGranularDate.SCHEMA$;
    }

    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return WordCount.SCHEMA$;
    }



    @Override
    protected void reduce(AvroKey<CompanyGranularDate> key, Iterable<AvroValue<WordCount>> values, Context context)
            throws IOException, InterruptedException {

        wordCounts.clear();
        sortedWordCountsList.clear();

        for (AvroValue<WordCount> value : values){
            WordCount wordCount = WordCount.newBuilder(value.datum()).build();

            String word = wordCount.getWord().toString();

            if (!stopWords.isStopWord(word)){

                if (wordCounts.containsKey(word)){
                    wordCounts.put(word, wordCounts.get(word) + 1);
                }else{
                    wordCounts.put(word, wordCount.getCount());
                }
            }
        }

        for (Map.Entry<String, Long> entry : wordCounts.entrySet()){
            sortedWordCountsList.add(new WordCount(entry.getKey(), entry.getValue()));
        }

        Collections.sort(sortedWordCountsList, new Comparator<WordCount>() {
            @Override
            public int compare(WordCount wordCount1, WordCount wordCount2) {
                return wordCount2.getCount().compareTo(wordCount1.getCount());
            }
        });

        sortedWordCountsValue.setSortedWordCounts(sortedWordCountsList.subList(0, Math.min(1000,sortedWordCountsList.size())));


        context.write(key, new AvroValue<SortedWordCounts>(sortedWordCountsValue));

    }

}
