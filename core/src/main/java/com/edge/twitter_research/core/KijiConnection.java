package com.edge.twitter_research.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.kiji.schema.*;
import org.kiji.schema.layout.KijiTableLayout;

import java.io.FileInputStream;
import java.io.IOException;


public class KijiConnection extends Configured {

    public KijiTable kijiTable = null;
    private CrisisMailer crisisMailer;


    public KijiConnection(String tableName){
        Kiji kiji;
        crisisMailer = CrisisMailer.getCrisisMailer();

        try{
            setConf(HBaseConfiguration
                    .addHbaseResources(new Configuration(true)));
            kiji = Kiji.Factory.open(
                    KijiURI.newBuilder()
                            .withInstanceName(KConstants.DEFAULT_INSTANCE_NAME)
                            .build(),
                    getConf());
            kiji.openTable(tableName);
        } catch (IOException ioException){
            ioException.printStackTrace();
            crisisMailer.sendEmailAlert(ioException);
        }
    }


    public KijiConnection(String tableLayoutPath, String tableName){
        Kiji kiji = null;
        crisisMailer = CrisisMailer.getCrisisMailer();

        try{
            setConf(HBaseConfiguration
                    .addHbaseResources(new Configuration(true)));
            kiji = Kiji.Factory.open(
                    KijiURI.newBuilder()
                            .withInstanceName(KConstants.DEFAULT_INSTANCE_NAME)
                            .build(),
                    getConf());
            KijiTableLayout kijiTableLayout =
                    KijiTableLayout
                            .createFromEffectiveJson(new FileInputStream(tableLayoutPath));
            kiji.createTable(tableName, kijiTableLayout);
            kijiTable = kiji.openTable(tableName);

        }catch (Exception exception){
            /*Checking for 2 exceptions because of Kiji bug*/
            if (exception instanceof KijiAlreadyExistsException||
                    exception instanceof RuntimeException){
                try{
                    kijiTable = kiji.openTable(tableName);
                }catch (IOException ioException){
                    ioException.printStackTrace();
                    crisisMailer.sendEmailAlert(ioException);
                }
            } else{
                exception.printStackTrace();
                crisisMailer.sendEmailAlert(exception);
            }
        }
    }
}
