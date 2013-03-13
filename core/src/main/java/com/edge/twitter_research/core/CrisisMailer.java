package com.edge.twitter_research.core;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.log4j.Logger;

import java.util.Properties;


public final class CrisisMailer {

    private static CrisisMailer crisisMailer = null;
    private static Session emailSession;
    private static Logger logger =
            Logger.getLogger(CrisisMailer.class);


    private CrisisMailer(){
        Properties props = new Properties();
        props.put(GlobalConstants.MAIL_SMTP_AUTH_PROPERTY,
                GlobalConstants.MAIL_SMTP_AUTH_VALUE);
        props.put(GlobalConstants.MAIL_SMTP_STARTTLS_ENABLE_PROPERTY,
                    GlobalConstants.MAIL_SMTP_STARTTLS_ENABLE_VALUE);
        props.put(GlobalConstants.MAIL_SMTP_HOST_PROPERTY,
                    GlobalConstants.MAIL_SMTP_HOST_VALUE);
        props.put(GlobalConstants.MAIL_SMTP_PORT_PROPERTY,
                GlobalConstants.MAIL_SMTP_PORT_VALUE);

        emailSession = Session.getInstance(props,
                new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(GlobalConstants.EMAIL_USERNAME,
                                                            GlobalConstants.EMAIL_PASSWORD);
                    }
                });
    }


    public static CrisisMailer getCrisisMailer(){
        if (crisisMailer == null){
            crisisMailer = new CrisisMailer();
        }
        return crisisMailer;
    }


    public void sendEmailAlert(Exception exception){
        try {
            Message message = new MimeMessage(emailSession);
            message.setFrom(new InternetAddress(GlobalConstants.EMAIL_USERNAME));
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse(GlobalConstants.REPORTING_EMAIL_USERNAME));
            message.setSubject("Error: " + exception.getMessage());
            message.setText(exception.toString() + "\n" + getStackTraceAsString(exception));

            Transport.send(message);
        } catch (MessagingException messagingException) {
            logger.error("Exception while sending an alert email",
                        messagingException);
        }
    }


    private String getStackTraceAsString(Exception exception){
        String trace = "";
        for (StackTraceElement stackTraceElement : exception.getStackTrace())
            trace += stackTraceElement.toString() + "\n";
        return trace;
    }
}
