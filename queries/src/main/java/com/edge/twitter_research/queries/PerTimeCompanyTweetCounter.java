package com.edge.twitter_research.queries;



import com.edge.twitter_research.core.CompanyData;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.collections.keyvalue.TiedMapEntry;
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
import java.util.HashMap;
import java.util.Map;

public class PerTimeCompanyTweetCounter
        extends KijiReducer<AvroKey<GranularDate>, AvroValue<CompanyOccurrenceType>,
                            AvroKey<GranularDate>, AvroValue<CompanyTweetCountsInTimeUnit>>
        implements AvroKeyWriter, AvroValueWriter, AvroValueReader, AvroKeyReader {

    private HashMap<CompanyData, TweetTypes> companyOccurrences;
    private ArrayList<CompanyOccurrenceCount> companyTweetCountInTimeUnitArrayList;
    private CompanyTweetCountsInTimeUnit companyTweetCountsInTimeUnit;

    public Schema getAvroKeyReaderSchema() throws IOException {
        return GranularDate.SCHEMA$;
    }

    public Schema getAvroValueReaderSchema() throws IOException {
        return CompanyOccurrenceType.SCHEMA$;
    }

    public Schema getAvroKeyWriterSchema() throws IOException {
        return GranularDate.SCHEMA$;
    }

    public Schema getAvroValueWriterSchema() throws IOException {
        return CompanyTweetCountsInTimeUnit.SCHEMA$;
    }

    public void setup(Context context)throws IOException, InterruptedException{
        super.setup(context);

        companyOccurrences = new HashMap<CompanyData, TweetTypes>();
        companyTweetCountInTimeUnitArrayList = new ArrayList<CompanyOccurrenceCount>();
        companyTweetCountsInTimeUnit = new CompanyTweetCountsInTimeUnit();
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
    protected void reduce(AvroKey<GranularDate> key, Iterable<AvroValue<CompanyOccurrenceType>> values, Context context)
            throws IOException, InterruptedException {

        companyOccurrences.clear();
        companyTweetCountInTimeUnitArrayList.clear();

        for (AvroValue<CompanyOccurrenceType> value : values){
            CompanyOccurrenceType companyOccurrenceType = CompanyOccurrenceType.newBuilder(value.datum()).build();
            CompanyData companyData = new CompanyData(companyOccurrenceType.getCompanyName(),
                    companyOccurrenceType.getCompanyArea());

            if (companyOccurrences.containsKey(companyData)){
                TweetTypes count = companyOccurrences.get(companyData);
                if (companyOccurrenceType.getIsRetweet())
                    count.setRetweetCount(count.getRetweetCount() + 1);
                else
                    count.setRegularCount(count.getRegularCount() + 1);
            }else{
                TweetTypes count = new TweetTypes();
                if (companyOccurrenceType.getIsRetweet()){
                    count.setRetweetCount(1);
                    count.setRegularCount(0);
                }
                else{
                    count.setRegularCount(1);
                    count.setRetweetCount(0);
                }
                companyOccurrences.put(companyData, count);
            }
        }

        for (Map.Entry<CompanyData, TweetTypes> companyOccurrence : companyOccurrences.entrySet()){
            companyTweetCountInTimeUnitArrayList
                    .add(new CompanyOccurrenceCount(companyOccurrence.getKey().getCompanyName(),
                                                    companyOccurrence.getKey().getCompanyArea(),
                                                    companyOccurrence.getValue()));
        }

        companyTweetCountsInTimeUnit.setCompanyTweetCountsInTimeUnit(companyTweetCountInTimeUnitArrayList);

        context.write(key, new AvroValue<CompanyTweetCountsInTimeUnit>(companyTweetCountsInTimeUnit));
    }
}

