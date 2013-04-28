package com.edge.twitter_research.queries;


import com.edge.twitter_research.core.CompanyData;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

import java.io.IOException;

public class TimeCompanyOccurrencesMapper
        extends KijiMapper<AvroKey<GranularDate>, AvroValue<CompanyTweetCountsInTimeUnit>,
                            AvroKey<CompanyData>, AvroValue<CompanyTweetCountInTimeUnit>>
        implements AvroKeyReader, AvroKeyWriter, AvroValueWriter, AvroValueReader {

    private CompanyData companyData;
    private CompanyTweetCountInTimeUnit companyTweetCountInTimeUnit;

    @Override
    public Class<?> getOutputValueClass() {
        return AvroValue.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return AvroKey.class;
    }

    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        return CompanyData.SCHEMA$;
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return CompanyTweetCountInTimeUnit.SCHEMA$;
    }

    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return GranularDate.SCHEMA$;
    }

    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return CompanyTweetCountsInTimeUnit.SCHEMA$;
    }

    @Override
    public void setup(Context context)
        throws IOException, InterruptedException{
        super.setup(context);

        companyData = new CompanyData();
        companyTweetCountInTimeUnit = new CompanyTweetCountInTimeUnit();
    }


    @Override
    public void map(AvroKey<GranularDate> key, AvroValue<CompanyTweetCountsInTimeUnit> value, Context context)
            throws IOException, InterruptedException {

        GranularDate granularDate = GranularDate.newBuilder(key.datum()).build();
        CompanyTweetCountsInTimeUnit companyTweetCountsInTimeUnit =
                CompanyTweetCountsInTimeUnit.newBuilder(value.datum()).build();

        for (CompanyOccurrenceCount companyOccurrenceCount :
                companyTweetCountsInTimeUnit.getCompanyTweetCountsInTimeUnit()){
            companyData.setCompanyName(companyOccurrenceCount.getCompanyName());
            companyData.setCompanyArea(companyOccurrenceCount.getCompanyArea());

            companyTweetCountInTimeUnit.setGranularDate(granularDate);
            companyTweetCountInTimeUnit.setCount(companyOccurrenceCount.getCount());

            context.write(new AvroKey<CompanyData>(companyData),
                    new AvroValue<CompanyTweetCountInTimeUnit>(companyTweetCountInTimeUnit));

        }
    }
}
