package com.edge.twitter_research.collector_filter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.CrisisMailer;
import com.edge.twitter_research.core.Timer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;


public class PhraseFetchingThread extends Thread {

    private BufferedReader bufferedReader;
    private FileReader fileReader;
    private String phraseFilePath;
    private HashSet<String> phrases;
    private TwitterStream phraseCollector;

    private LinkedBlockingQueue<Status> tweetStorageQueue;
    private Configuration configuration;

    private static Logger logger
            = Logger.getLogger(PhraseFetchingThread.class);
    private CrisisMailer crisisMailer;


    public PhraseFetchingThread(String PhraseFilePath,
                                LinkedBlockingQueue<Status> tweetStorageQueue,
                                Configuration configuration){
        PropertyConfigurator.configure(this.getClass()
                .getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));
        crisisMailer = CrisisMailer.getCrisisMailer();
        phrases = new HashSet<String>();
        phraseFilePath = PhraseFilePath;
        phraseCollector = null;

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
            int numPhrases = phrases.size();

            while (true){
                try{
                    phrase = bufferedReader.readLine();
                    if (phrase == null)
                        break;

                    phrases.add(phrase);

                }catch (IOException ioException){
                    logger.error("IOException while reading line", ioException);
                    crisisMailer.sendEmailAlert(ioException);
                }
            }

            try{
                if (numPhrases < phrases.size()){
                    if (phraseCollector != null){
                        phraseCollector.shutdown();
                        phraseCollector.cleanUp();
                    }
                    startCollectorForPhrases();
                    logger.warn("Started collector for " + toString());
                }
            }catch (Exception unknownException){
                logger.error("Unknown Exception while shutting down or starting a phraseCollector",
                        unknownException);
                crisisMailer.sendEmailAlert(unknownException);
                break;
            }


            try{
                closeFile();
            }catch (IOException ioException){
                logger.error("Unable to close phrases file", ioException);
                crisisMailer.sendEmailAlert(ioException);
                break;
            }

            Timer.putToSleep(Constants.PHRASE_FILE_CHECKING_INTERVAL);
        }

        logger.error("PhraseFetchingThread has stopped of own free will");
        crisisMailer.sendEmailAlert("collector_filter: PhraseFetchingThread has stopped of own free will");
    }


    private void startCollectorForPhrases(){
        GetStatusesFilterStreamListener listener =
                    new GetStatusesFilterStreamListener(tweetStorageQueue);
        phraseCollector =
                    new TwitterStreamFactory(configuration).getInstance();
        phraseCollector.addListener(listener);

        phraseCollector.filter(new FilterQuery(0, new long[0],
                                    phrases.toArray(new String[phrases.size()])));
    }


    public String toString(){
        String str = "";
        for (String phrase : phrases){
            str += phrase + " ";
        }
        return str;
    }
}
