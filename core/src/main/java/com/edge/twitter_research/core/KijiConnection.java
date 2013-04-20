package com.edge.twitter_research.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.log4j.PropertyConfigurator;
import org.kiji.schema.*;
import org.kiji.schema.layout.KijiTableLayout;

import org.apache.log4j.Logger;
import org.kiji.schema.util.ResourceUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


public class KijiConnection extends Configured {

    private Kiji kiji;
    public KijiTable kijiTable;
    public KijiTableWriter kijiTableWriter;
    public KijiTableReader kijiTableReader;

    private CrisisMailer crisisMailer;
    private Logger logger;


    private void initializeKijis(){
        crisisMailer = CrisisMailer.getCrisisMailer();
        logger = Logger.getLogger(KijiConnection.class);

        kiji = null;
        kijiTable = null;
        kijiTableWriter = null;
        kijiTableReader = null;


        try{
            setConf(HBaseConfiguration
                    .addHbaseResources(new Configuration(true)));
        }catch (Exception unknownException){
            logger.error("Unknown Exception while setting HBase Configuration",
                    unknownException);
            crisisMailer.sendEmailAlert(unknownException);
        }
    }


    public boolean isValidKijiConnection(){
        return (kiji != null &&
                kijiTable != null &&
                kijiTableReader != null &&
                kijiTableWriter != null);
    }


    private void nullifyKijis(){

        try{
            if (kijiTableReader != null){
                kijiTableReader.close();
                kijiTableReader = null;
            }
            if (kijiTableWriter != null){
                kijiTableWriter.close();
                kijiTableWriter = null;
            }
            if (kijiTable != null){
                kijiTable.release();
                kijiTable = null;
            }
            if (kiji != null){
                kiji.release();
                kiji = null;
            }
        }catch (IOException ioException){
            logger.error("IOException while closing/releasing KijiConnection member objects",
                    ioException);
        }
    }


    public void destroy(){
        nullifyKijis();
    }


    public KijiConnection(String tableName){
        initializeKijis();

        try{
            kiji = Kiji.Factory.open(
                    KijiURI.newBuilder()
                            .withInstanceName(KConstants.DEFAULT_INSTANCE_NAME)
                            .build(),
                    getConf());
            kijiTable =  kiji.openTable(tableName);
            kijiTableReader = kijiTable.openTableReader();
            kijiTableWriter = kijiTable.openTableWriter();

        } catch (Exception exception){
            if (exception instanceof IOException){
                logger.error("IOException while initializing KijiTable stuff",
                        exception);
            }else{
                logger.error("Unknown Exception while initializing KijiTable stuff",
                        exception);
            }
            nullifyKijis();
            crisisMailer.sendEmailAlert(exception);
        }
    }


    public KijiConnection(InputStream tableLayoutPath,
                          String tableName){
        initializeKijis();

        try{
            kiji = Kiji.Factory.open(
                    KijiURI.newBuilder()
                            .withInstanceName(KConstants.DEFAULT_INSTANCE_NAME)
                            .build(),
                    getConf());

            KijiTableLayout kijiTableLayout =
                    KijiTableLayout
                            .createFromEffectiveJson(tableLayoutPath);

            kiji.createTable(kijiTableLayout.getDesc());

            kijiTable = kiji.openTable(tableName);

            kijiTableReader = kijiTable.openTableReader();
            kijiTableWriter = kijiTable.openTableWriter();

        }catch (Exception exception){
            if (exception instanceof KijiAlreadyExistsException ||
                    exception instanceof RuntimeException){
                try{
                    logger.error(exception);
                    kijiTable = kiji.openTable(tableName);
                    kijiTableReader = kijiTable.openTableReader();
                    kijiTableWriter = kijiTable.openTableWriter();

                }catch (Exception internalException){
                    if (internalException instanceof IOException){
                        logger.error("IOException while opening Kiji Table",
                                    internalException);
                    }else {
                        logger.error("Unknown Exception while opening Kiji Table",
                                    internalException);
                    }
                    nullifyKijis();
                    crisisMailer.sendEmailAlert(internalException);
                }
            } else{
                logger.error("Unknown Exception while initializing Kiji Stuff",
                        exception);
                crisisMailer.sendEmailAlert(exception);
                nullifyKijis();
            }
        }
    }
}
