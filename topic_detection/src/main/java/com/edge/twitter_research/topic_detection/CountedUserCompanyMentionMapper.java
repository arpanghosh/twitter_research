package com.edge.twitter_research.topic_detection;


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

public class CountedUserCompanyMentionMapper
        extends KijiMapper<AvroKey<UserCompany>, AvroValue<UserCount>, AvroKey<CompanyData>, AvroValue<UserCount>>
        implements AvroKeyWriter, AvroValueWriter, AvroKeyReader, AvroValueReader {

    CompanyData companyData;

    @Override
    public void setup(Context context)
        throws IOException, InterruptedException{
        super.setup(context);

        companyData = new CompanyData();
    }

    @Override
    public void map(AvroKey<UserCompany> key, AvroValue<UserCount> value, Context context)
            throws IOException, InterruptedException {

        companyData.setCompanyName(key.datum().getCompanyName());
        companyData.setCompanyArea(key.datum().getCompanyArea());

        context.write(new AvroKey<CompanyData>(companyData), value);
    }


    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        return CompanyData.SCHEMA$;
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return UserCount.SCHEMA$;
    }


    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return UserCompany.SCHEMA$;
    }


    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return UserCount.SCHEMA$;
    }

    @Override
    public Class<?> getOutputValueClass() {
        return AvroValue.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return AvroKey.class;
    }
}


