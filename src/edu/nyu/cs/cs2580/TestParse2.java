import java.io.File;
import org.jsoup.*;
import org.jsoup.nodes.*;
import org.jsoup.select.*;
 
public class TestParse2
{
	public static String getPlainText(String filename)
	{
		String plain = "";
		try 
		{
			File input = new File(filename);
			Document doc = Jsoup.parse(input, "UTF-8", "");

			Elements bodys = doc.getElementsByTag("body");
			
			for (Element body : bodys) 
			{
				plain = body.text();
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally
		{
			return plain;
		}
		
	}
	
	public static void main(String argv[]) 
	{
		System.out.println(TestParse2.getPlainText("Alaska"));
	
		
	}
 
}