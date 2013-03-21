package com.edge.twitter_research.relevance_filter;

import com.edge.twitter_research.core.KijiConnection;
import org.apache.hadoop.hbase.HConstants;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

public class DepthChecker {

    public static void main(String[] args){
        if (args.length < 3){
            System.out.println("Usage: DepthChecker " +
                    "<use_paging> " +
                    "<row_key> " +
                    "<paging_size> " +
                    "<max_num_versions>");
            return;
        }

        String rowKey = args[1];
        boolean usePaging = Boolean.parseBoolean(args[0]);
        int pageSize = Integer.parseInt(args[2]);
        int maxVersions = HConstants.ALL_VERSIONS;
        if (args.length == 4)
            maxVersions = Integer.parseInt(args[3]);

        /*My own helper class which initializes Kiji, KijiTable, KijiTableReader/Writer for a given
        table name
         */
        KijiConnection kijiConnection = new KijiConnection(Constants.EMOTICON_STORE_TABLE_NAME);

        KijiDataRequestBuilder builder = KijiDataRequest.builder();

        if (usePaging){
            builder.newColumnsDef()
                    .withMaxVersions(maxVersions)
                    .withPageSize(pageSize)
                    .add("emoticon_occurrence", "tweet_id");
        }else{
            builder.newColumnsDef()
                    .withMaxVersions(maxVersions)
                    .add("emoticon_occurrence", "tweet_id");
        }

        long depth = 0L;
        try{
            KijiRowData rowData =
                    kijiConnection.kijiTableReader.get(kijiConnection.kijiTable.getEntityId(rowKey),
                    builder.build());

            if (usePaging){
                KijiPager kijiPager = rowData.getPager("emoticon_occurrence", "tweet_id");
                while (kijiPager.hasNext()){
                    depth += kijiPager.next().getValues("emoticon_occurrence", "tweet_id").size();
                }
                kijiPager.close();
            }else{
                depth += rowData.getValues("emoticon_occurrence", "tweet_id").size();
            }

        }catch (IOException ioException){
            System.out.println(ioException);
            ioException.printStackTrace();
        }

        System.out.println("Depth of " + rowKey + " is " + depth);
    }
}
