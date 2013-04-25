package com.edge.twitter_research.queries;



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

public class ReducerTemplate
        extends KijiReducer<IntWritable, Text, AvroKey<Integer>, AvroValue<TweetsToday>>
        implements AvroKeyWriter, AvroValueWriter {

    private HashMap<String, Integer> sets;

    public Schema getAvroKeyWriterSchema() throws IOException {
        return Schema.create(Schema.Type.INT);
    }

    public Schema getAvroValueWriterSchema() throws IOException {
        return TweetsToday.SCHEMA$;
    }

    public void setup(Context context)throws IOException, InterruptedException{
        super.setup(context);
        sets = new HashMap<String, Integer>();
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
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            for(Text company : values){
                String myCompany = company.toString();
                if (sets.containsKey(myCompany)){
                    int times = sets.get(myCompany);
                    sets.put(myCompany, times++);}
                else
                    sets.put(myCompany, 1);
            }
        ArrayList<CompanyOccuranceCount> counts = new ArrayList<CompanyOccuranceCount>();
        for (Map.Entry<String, Integer> entry : sets.entrySet()){
            counts.add(new CompanyOccuranceCount(entry.getKey(), entry.getValue()));
        }
        TweetsToday tweets = new TweetsToday(counts);

        context.write(new AvroKey<Integer>(key.get()), new AvroValue<TweetsToday>(tweets));

    }
}

