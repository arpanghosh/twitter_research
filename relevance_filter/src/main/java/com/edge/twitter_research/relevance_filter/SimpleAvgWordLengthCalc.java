package com.edge.twitter_research.relevance_filter;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class SimpleAvgWordLengthCalc {

    public static void main(String[] args){

        try{

            FileInputStream fstream = new FileInputStream(args[0]);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String strLine;

            double relevantTotal = 0;
            int numRelevant = 0;

            double notRelevantTotal = 0;
            int numNotRelevant = 0;

            br.readLine();

            while ((strLine = br.readLine()) != null)   {


                try{

                String[] tokens = strLine.split(",");

                double avgWordLength = Double.parseDouble(tokens[12]);
                int label =  Integer.parseInt(tokens[18]);

                if (label == 1){
                    relevantTotal += avgWordLength;
                    numRelevant++;
                }else if (label == 2){
                    notRelevantTotal += avgWordLength;
                    numNotRelevant++;

                }
                }catch (Exception e){
                    continue;
                }
            }

            System.out.println(args[0]);
            System.out.println("Relevant avg word length : " + relevantTotal/numRelevant);
            System.out.println("Not Relevant avg word length : " + notRelevantTotal/numNotRelevant);


            in.close();
        }catch (Exception e){//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
    }
}
