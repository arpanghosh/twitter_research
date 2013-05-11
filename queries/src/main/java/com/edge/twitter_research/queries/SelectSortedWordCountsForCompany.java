package com.edge.twitter_research.queries;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

import java.io.IOException;



public class SelectSortedWordCountsForCompany
        extends KijiMapper<AvroKey<CompanyGranularDate>, AvroValue<SortedWordCounts>,
        Text, AvroValue<GranularDateSortedWordCounts>>
        implements AvroKeyReader, AvroValueReader, AvroValueWriter {

    private GranularDateSortedWordCounts granularDateSortedWordCounts;

    @Override
    public Class<?> getOutputValueClass() {
        return AvroValue.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }


    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return CompanyGranularDate.SCHEMA$;
    }

    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return SortedWordCounts.SCHEMA$;
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return GranularDateSortedWordCounts.SCHEMA$;
    }


    @Override
    public void setup(Context context)
            throws IOException, InterruptedException{
        super.setup(context);

        granularDateSortedWordCounts = new GranularDateSortedWordCounts();
    }


    @Override
    public void map(AvroKey<CompanyGranularDate> key, AvroValue<SortedWordCounts> value, Context context)
            throws IOException, InterruptedException {

        CompanyGranularDate companyGranularDate = CompanyGranularDate.newBuilder(key.datum()).build();
        SortedWordCounts sortedWordCounts = SortedWordCounts.newBuilder(value.datum()).build();

        granularDateSortedWordCounts.setGranularDate(companyGranularDate.getDate());
        granularDateSortedWordCounts.setSortedWordCounts(sortedWordCounts);

        context.write(new Text(companyGranularDate.getCompanyName().toString()),
                new AvroValue<GranularDateSortedWordCounts>(granularDateSortedWordCounts));
    }
}