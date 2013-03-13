package com.edge.twitter_research.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.log4j.PropertyConfigurator;
import org.kiji.schema.*;
import org.kiji.schema.layout.KijiTableLayout;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;


public class KijiConnection extends Configured {

    public KijiTable kijiTable = null;

    private CrisisMailer crisisMailer;
    private Logger logger;


    public KijiConnection(String tableName){
        Kiji kiji = null;

        crisisMailer = CrisisMailer.getCrisisMailer();
        logger = Logger.getLogger(KijiConnection.class);

        try{
            setConf(HBaseConfiguration
                    .addHbaseResources(new Configuration(true)));
            kiji = Kiji.Factory.open(
                    KijiURI.newBuilder()
                            .withInstanceName(KConstants.DEFAULT_INSTANCE_NAME)
                            .build(),
                    getConf());
            kiji.openTable(tableName);
        } catch (Exception exception){
            if (exception instanceof IOException){
                logger.error("Exception while opening a KijiTable",
                            exception);
            }else{
                logger.error("Exception while initializing a Kiji object",
                        exception);
            }
            crisisMailer.sendEmailAlert(exception);
        }
    }


    public KijiConnection(String tableLayoutPath,
                          String tableName){
        Kiji kiji = null;

        crisisMailer = CrisisMailer.getCrisisMailer();
        logger = Logger.getLogger(KijiConnection.class);

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
                }catch (Exception internalException){
                    if (internalException instanceof IOException){
                        logger.error("Exception while opening a KijiTable",
                                    internalException);
                    }else {
                        logger.error("Exception while initializing a Kiji object",
                                    internalException);
                    }
                    crisisMailer.sendEmailAlert(internalException);
                }
            } else{
                logger.error("Exception while opening a KijiTable",
                        exception);
                crisisMailer.sendEmailAlert(exception);
            }
        }
    }
}
