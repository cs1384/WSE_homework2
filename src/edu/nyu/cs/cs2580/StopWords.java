/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.nyu.cs.cs2580;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

/**
 *
 * @author Ali Local
 */
public class StopWords
{
    ArrayList<String> words;
    public StopWords(Options options)
    {
        System.out.println("Stopwords");
        words = new ArrayList<String>();
        try
        {
            BufferedReader bf = new BufferedReader(new FileReader(new File("stopwords.txt")));
            String line;
            while((line = bf.readLine()) != null)
            {
                System.out.println(line);
                words.add(line);
            }
            System.out.println("Added stopwords");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
                
        
    }
    
    public boolean wordInList(String word)
    {
        return (words.contains(word));
    }
}
