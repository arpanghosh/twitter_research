package com.edge.twitter_research.collector_user;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import com.edge.twitter_research.core.CrisisMailer;
import com.edge.twitter_research.core.Timer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class UserFetchingThread extends Thread {

    private BufferedReader bufferedReader;
    private FileReader fileReader;
    private String userFilePath;
    private HashSet<String> companies;

    private LinkedBlockingQueue<UserCompanyMessage> outputQueue;

    private static Logger logger
            = Logger.getLogger(UserFetchingThread.class);
    private CrisisMailer crisisMailer;


    public UserFetchingThread(String PhraseFilePath,
                                LinkedBlockingQueue<UserCompanyMessage> outputQueue){
        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);
        crisisMailer = CrisisMailer.getCrisisMailer();
        companies = new HashSet<String>();
        userFilePath = PhraseFilePath;

        this.outputQueue = outputQueue;
    }


    public void openFile() throws FileNotFoundException{
        fileReader = new FileReader(userFilePath);
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
                logger.error("Users file not found", fileNotFoundException);
                crisisMailer.sendEmailAlert(fileNotFoundException);
                break;
            }



            try{

                String companyName = bufferedReader.readLine();
                String companyArea = bufferedReader.readLine();

                if (!companies.contains(companyName)){
                    companies.add(companyName);

                    while (true){
                        String userID = bufferedReader.readLine();
                        if(userID == null)
                            break;

                        outputQueue.add(new UserCompanyMessage(Long.parseLong(userID),
                                companyName, companyArea));
                    }
                }

            }catch (IOException ioException){
                logger.error("IOException while reading line", ioException);
                crisisMailer.sendEmailAlert(ioException);
            }


            try{
                closeFile();
            }catch (IOException ioException){
                logger.error("Unable to close users file", ioException);
                crisisMailer.sendEmailAlert(ioException);
                break;
            }

            Timer.putToSleep(Constants.USER_FILE_CHECKING_INTERVAL);
        }

        logger.error("UserFetchingThread has stopped of own free will");
        crisisMailer.sendEmailAlert("collector_user: UserFetchingThread has stopped of own free will");
    }
}
