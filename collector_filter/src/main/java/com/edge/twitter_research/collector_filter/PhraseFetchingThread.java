package com.edge.twitter_research.collector_filter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.CrisisMailer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;


public class PhraseFetchingThread extends Thread {

    private BufferedReader bufferedReader;
    private FileReader fileReader;
    private String phraseFilePath;
    private HashSet<String> phrases;

    private LinkedBlockingQueue<TweetPhraseMessage> tweetStorageQueue;
    private String log4jPropertiesFilePath;
    private Configuration configuration;

    private static Logger logger
            = Logger.getLogger(PhraseFetchingThread.class);
    private CrisisMailer crisisMailer;


    public PhraseFetchingThread(String log4jPropertiesFilePath,
                                String PhraseFilePath,
                                LinkedBlockingQueue<TweetPhraseMessage> tweetStorageQueue,
                                Configuration configuration){
        PropertyConfigurator.configure(log4jPropertiesFilePath);
        crisisMailer = CrisisMailer.getCrisisMailer();
        phrases = new HashSet<String>();
        phraseFilePath = PhraseFilePath;


        this.log4jPropertiesFilePath = log4jPropertiesFilePath;
        this.tweetStorageQueue = tweetStorageQueue;
        this.configuration = configuration;
    }


    public void openFile() throws FileNotFoundException{
        fileReader = new FileReader(phraseFilePath);
        bufferedReader = new BufferedReader(fileReader);
    }


    public void closeFile() throws IOException{
        bufferedReader.close();
        fileReader.close();
    }


    public void run() {
        while (true){
            try{
                openFile();
            }catch (FileNotFoundException fileNotFoundException){
                logger.error("Phrases file not found", fileNotFoundException);
                crisisMailer.sendEmailAlert(fileNotFoundException);
                break;
            }

            String phrase;

            while (true){
                try{
                    phrase = bufferedReader.readLine();
                    if (phrase == null)
                        break;

                    if (!phrases.contains(phrase)){
                        phrases.add(phrase);
                        startCollectorForPhrase(phrase);
                        logger.warn("Started collector for phrase: " + phrase);
                    }

                }catch (IOException ioException){
                    logger.error("IOException while reading line", ioException);
                    crisisMailer.sendEmailAlert(ioException);
                }
            }

            try{
                closeFile();
            }catch (IOException ioException){
                logger.error("Unable to close phrases file", ioException);
                crisisMailer.sendEmailAlert(ioException);
                break;
            }

            CollectorDriver.putToSleep(Constants.PHRASE_FILE_CHECKING_INTERVAL);
        }

        logger.error("PhraseFetchingThread has stopped of own free will");
        crisisMailer.sendEmailAlert("collector_filter: PhraseFetchingThread has stopped of own free will");
    }


    private void startCollectorForPhrase(String phrase){
        GetStatusesFilterStreamListener listener =
                    new GetStatusesFilterStreamListener(tweetStorageQueue,
                                                        log4jPropertiesFilePath,
                                                        phrase);
        TwitterStream twitterStream =
                    new TwitterStreamFactory(configuration).getInstance();
        twitterStream.addListener(listener);
        GetStatusesFilterStreamThread getStatusesFilterStreamThread =
                new GetStatusesFilterStreamThread(twitterStream,
                                                  phrase,
                                                    log4jPropertiesFilePath);
        getStatusesFilterStreamThread.start();
    }
}
