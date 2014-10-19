package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestParse3
{

    public static String getPlainText(File input)
    {
        String plain = "";
        try
        {
            BufferedReader bf = new BufferedReader(new FileReader(input));
            String line;
            String text = "";
            while((line = bf.readLine()) != null)
            {
                text += line;
            }
            Pattern myPattern = Pattern.compile("(.*<\\s*body[^>]*>)|(<\\s*/\\s*body\\s*\\>.+)", Pattern.DOTALL);
            
            Matcher m = myPattern.matcher(text);
            while (m.find()) 
            {
                String s = m.group(1);
                System.out.println(s);
                // s now contains "BAR"
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            return plain;
        }
    }

    public static String getPlainText(String filename)
    {
        String plain = "";
        try
        {
            File input = new File(filename);
            plain = getPlainText(input);
            
            plain = plain.replaceAll("[^a-zA-Z0-9\\s]", " ");
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            return plain;
        }

    }

    public static void main(String argv[])
    {
        System.out.println(TestParse3.getPlainText("E:\\CUNY Documents\\8. Fall 2014\\search\\hw2\\repo\\WSE_homework2\\data\\New folder\\Yiddish_language"));

    }

}
