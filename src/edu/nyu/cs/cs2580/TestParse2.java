package edu.nyu.cs.cs2580;

import java.io.File;
import org.jsoup.*;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

public class TestParse2
{

    public static String getPlainText(File input)
    {
        String plain = "";
        try
        {
            org.jsoup.nodes.Document doc = Jsoup.parse(input, "UTF-8", "");

            Elements bodys = doc.getElementsByTag("body");

            for (Element body : bodys)
            {
                plain = body.text();
                plain = plain.replaceAll("[^a-zA-Z0-9\\s]", " ");
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
        System.out.println(TestParse2.getPlainText("Alaska"));

    }

}
