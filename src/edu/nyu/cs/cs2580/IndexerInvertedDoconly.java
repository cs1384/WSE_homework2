package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Comparator;
import java.util.Collections;
import edu.nyu.cs.cs2580.SearchEngine.Options;

import java.io.File;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable {
  private static final long serialVersionUID = 1077111905740085032L;
  private int _roundtake;
  
  private class Record implements Serializable{
    int lineN = -1;
    //Vector<Integer> freq = new Vector<Integer>();
    public Record(int l){
      lineN = l;
      //Vector<Integer> freq = new Vector<Integer>();
    }
  }
  
  private Map<String, Vector<Integer>> _index =
      new HashMap<String, Vector<Integer>>();
  
  private Map<String, int []> _tindex =
	      new HashMap<String, int []>();
  
  private class Posting implements Serializable{
    public int did;
    //get occurance by offsets.size()
    //public Vector<Integer> offsets = new Vector<Integer>();
    public Posting(int did){
      this.did = did;
    }
  }
  
  private Map<String, Vector<Posting>> _op = 
      new TreeMap<String, Vector<Posting>>();
  //Frequency of each term in entire corpus, optional
  //private Map<Integer, Integer> _termCorpusFrequency = 
  //    new HashMap<Integer, Integer>();
  //map url to docid to support documentTermFrequency method,optional 
  private Map<String, Integer> _urlToDoc = new HashMap<String, Integer>(); 
  
  //to store and quick access to basic document information such as title 
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  //private int _uniqueTerms = 0;
  
  //Provided for serialization
  public IndexerInvertedDoconly() { }
  //constructor
  public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }
  
  public static final Comparator<Posting> Comparator = new Comparator<Posting>()
		    {

		        @Override
		        public int compare(Posting o1, Posting o2) 
		        {
		            if(o1.did == o2.did) return 0;
		            return (o1.did < o2.did) ? -1 : 1;	//To sort in descending order
		        }

		    };
  
  public  void mergeIndices(int id) 
  {
      try
      {
          BufferedReader br1 = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/partial_cmpr_corpus_" + id + ".txt")));
          BufferedReader br2 = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_" + id + ".txt")));
          
          BufferedWriter outBw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_" + (id+1) + ".txt")));
          
          //now walk the files, and write to a new file
          int i=1, j=1;
          String file1Line = br1.readLine();
          String file2Line = br2.readLine();
              
          while(file1Line != null && file2Line != null)
          {
              String tokens1[] = file1Line.split(" ");
              String tokens2[] = file2Line.split(" ");
              
              String word1 = tokens1[0];
              String word2 = tokens2[0];
              if(word1.compareTo(word2) < 0)
              {
                  outBw.write(file1Line + "\n");
                  file1Line = br1.readLine();
                  i++;
              }
              else if(word1.compareTo(word2) > 0)
              {
                  outBw.write(file2Line + "\n");
                  file2Line = br2.readLine();
                  j++;
              }
              else
              {
            	  
            	  StringBuilder sb = new StringBuilder();
                  sb.append(word1);
                  for(int k=1;k<tokens2.length;k++){
                    sb.append(" ").append(tokens2[k]);
                  }
                  for(int k=1;k<tokens1.length;k++){
                      sb.append(" ").append(tokens1[k]);
                    }
                  outBw.write(sb.toString()+"\n");
                  file1Line = br1.readLine();
                  file2Line = br2.readLine();
              }
              
          }
          
          while(file1Line != null)
          {
              outBw.write(file1Line + "\n");
              file1Line = br1.readLine();
          }
          while(file2Line != null)
          {
              outBw.write(file2Line + "\n");
              file2Line = br2.readLine();
              j++;                
          }
          outBw.close();
          br1.close();
          br2.close();
          
          //delete
          File f1 = new File(_options._indexPrefix + "/partial_cmpr_corpus_" + id + ".txt");
          File f2 = new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_" + id + ".txt");
          
          f1.delete();
          f2.delete();
          
          
      }
      catch(Exception e)
      {
          e.printStackTrace();
      }
  }
  
  
  public void mergeAllFiles(int count){
	  System.out.println("Merging files...");
      try
      {
          File f = new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_0.txt");
          f.createNewFile();            

          for(int i=0;i<count;i++)
          {
              mergeIndices(i) ;
              System.out.println("Merged " + (i+1) + " / " + count);
          }
      }
      catch(Exception e)
      {
          e.printStackTrace();;
      } 
  }
  
  
  
  @Override
  public void constructIndex() throws IOException
  {
    printRuntimeInfo("======= GO WSE_homework2!! =======");
    try
    {
      String corpusFolder = _options._corpusPrefix + "/";
      System.out.println("Construct index from: " + corpusFolder);
      File folder = new File(corpusFolder);
      ArrayList<File> fileList = new ArrayList<File>();
      for (final File file : folder.listFiles()){
        fileList.add(file);
      }
      
      int lower=0, upper = 150;
      int takeTurn = 0;
      while(lower < fileList.size()){
        System.out.println("range:  low = " + lower + " , upper = " + upper);
        if(upper > fileList.size()){
          upper = fileList.size();
        }
        constructPartialIndex(fileList.subList(lower, upper));
        writeToIndexFile(takeTurn);
        //mergeFiles(takeTurn); //merge previous output files
        _op.clear();
        lower = upper;
        upper += 150;
        takeTurn = takeTurn + 1;
      }
      _roundtake = takeTurn;
      mergeAllFiles(takeTurn);
          }catch (Exception e){
        e.printStackTrace();
    }
    
    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with "
        + Long.toString(_totalTermFrequency) + " terms.");
    //System.out.println(_uniqueTerms);

    String indexFile = _options._indexPrefix + "/DoconlyIndexer.idx";
    System.out.println("Store index to: " + indexFile);
    ObjectOutputStream writer
        = new ObjectOutputStream(new FileOutputStream(indexFile));
    writer.writeObject(this); //write the entire class into the file
    writer.close();
      
    printRuntimeInfo("======== END!! ========");
  }
  

private void constructPartialIndex(List<File> listOfFiles){ 	
    //Map<String, Vector<Integer>> _opTemp = new HashMap<String, Vector<Integer>>();
    try{
      int count = 0;
      for (File file : listOfFiles){
        //System.out.println(file.getName());
        String text = TestParse2.getPlainText(file);
        processDocument(text,file.getName()); //process each webpage
        count++; 
        if(count % 150 == 0){
          printRuntimeInfo("====== 150 files =======");
        }
      }
    }catch(Exception e){
      e.printStackTrace();
    }

    System.out.println("Partially Indexed " + Integer.toString(_numDocs) + " docs with " + Long.toString(_totalTermFrequency) + " terms.");

  }
  
  public void writeToIndexFile(int takeTurn){
    BufferedWriter bw = null;
    try {
      bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/partial_cmpr_corpus_" + takeTurn + ".txt")));
      Vector<Posting> pv;
      StringBuilder sb = new StringBuilder(); 
      for(String term : _op.keySet()){
        sb.append(term);
        pv = _op.get(term);
        for(Posting p : pv)
        {
          sb.append(" ").append(p.did);
        }
        //System.out.println(sb.toString());
        sb.append("\n");
      }
      bw.write(sb.toString());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally{
      try {
        bw.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  
    
  public void makeIndex(){
    try{
    	
      
      BufferedReader br = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_"+ _roundtake +".txt")));
      String line = br.readLine();
      //Vector<Integer> freq = new Vector<Integer>();
      int i, op;
      String[] list;
      String term;
      printRuntimeInfo("========Loading indices=======");
      while(line!=null){
    	//System.out.println(line);
    	//Vector<Integer> freq = new Vector<Integer>();
        //freq.clear();
        //Scanner st = new Scanner(line);
        //String term = st.next();
        list = line.split(" ");
    	/*
        while (st.hasNext()){
        	this._totalTermFrequency++;
        	freq.add(Integer.parseInt(st.next()));
        }
        freq.trimToSize();
        _index.put(term,freq);
        line = br.readLine();
        st.close();
        */
        term = list[0];
        int [] freq = new int [list.length - 1];
        this._totalTermFrequency += (list.length-1);
        for(i=1;i<list.length;i++){
          op = Integer.parseInt(list[i]);
          freq[i-1] = op;
        }
        line = br.readLine();
        _tindex.put(term, freq);
      }
      br.close();
      printRuntimeInfo("======== END!! ========");
    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void processDocument(String content, String filename){
    
    //docid starts from 1
    DocumentIndexed doc = new DocumentIndexed(_documents.size() + 1);
    
    doc.setTitle(filename.replace('_', ' '));
    ProcessTerms(content, doc._docid);
    int numViews = (int) (Math.random() * 10000);
    doc.setNumViews(numViews);

    String url = "en.wikipedia.org/wiki/" + filename;
    doc.setUrl(url);
    //build up urlToDoc map
    _urlToDoc.put(filename, doc._docid);
    _documents.add(doc);
    _numDocs++;
    return;
  }
  
  public void ProcessTerms(String content, int docid){
    //map for the process of this doc
    //int offset = 1; //offset starts from 1
    Scanner s = new Scanner(content);
    Vector<String> docToken = new Vector<String>();
    String token;
    while (s.hasNext()){
    	token = s.next().toLowerCase().trim();
    	if (docToken.contains(token) == false){
    		docToken.add(token);
    	}
    }
    s.close();
    
    for (String token1: docToken){
    	if(_op.containsKey(token1)){
    		Posting posting = new Posting(docid);
    		_op.get(token1).add(posting);
    	}else{
    		Posting posting = new Posting(docid);
    		Vector<Posting> list = new Vector<Posting>();
            list.add(posting);
            _op.put(token1, list);
    	}
    	_totalTermFrequency++;
    }
    
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    String indexFile = _options._indexPrefix + "/DoconlyIndexer.idx";
    System.out.println("Load index from: " + indexFile);

    ObjectInputStream reader =
        new ObjectInputStream(new FileInputStream(indexFile));
    IndexerInvertedDoconly loaded = 
        (IndexerInvertedDoconly) reader.readObject();

    this._documents = loaded._documents;
    this._op = loaded._op;
    this._roundtake = loaded._roundtake;
    makeIndex();
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
   
    this._urlToDoc = loaded._urlToDoc;
    reader.close();
    
    System.out.println(Integer.toString(_numDocs) + " documents loaded " +
        "with " + Long.toString(_totalTermFrequency) + " terms!");
    
  }

  @Override
  public Document getDoc(int docid) {
    return (docid > _documents.size() || docid <= 0) ? 
        null : _documents.get(docid);
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}.
   */

  @Override
  public Document nextDoc(QueryPhrase query, int docid) {
      Vector<String> words = query._tokens;
      int length = words.size();
      Vector<Integer> newidlist = new Vector<Integer>();
      boolean flag = true;
      int nextid = 0;
      for (int i=0; i<length; i++){
    	  nextid = NextDocSingle(words.get(i), docid);
    	  if (nextid == 99999999){
    		  return null;}
    	  newidlist.add(nextid);
    	  if (i>=1 && nextid != newidlist.elementAt(i-1)){
    		  flag = false;
    	  }
      }
    	  
    	  if (flag == true){
    		  DocumentIndexed result = new DocumentIndexed(nextid);
    		  return result;
    	  }else{
    		  return nextDoc(query, Collections.max(newidlist));
    }
      
  }
  
  @Override
  public int corpusDocFrequencyByTerm(String term) {
      int freq = 0;
	  if (_index.containsKey(term)){
    	  freq = _index.get(term).size();
      }
	  return freq;
  }

  @Override
  public int corpusTermFrequency(String term) {
	  int freq = 0;
	  if (_index.containsKey(term)){
    	  freq = _index.get(term).size();
      }
	  return freq;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }
  
  private int NextDocSingle(String query, int current){
	Vector<Integer> postingList = _index.get(query);
	int postingLength = postingList.size();
	int minID = postingList.firstElement();
	int maxID = postingList.lastElement();
	if (maxID == 0 || current >= maxID){
		return 99999999;
	}else if (minID > current){
		return minID;
	}else{
		int i = 0;
		while(postingList.get(i) <= current){
			i++;
		}
		return postingList.get(i);
	}
  }
  
  private void Docall(QueryPhrase query){
	  int i = -1;
	  System.out.println("All the files that contain words: " + query._tokens);
	  DocumentIndexed result = (DocumentIndexed) nextDoc(query, i);
	  System.out.println(result._docid);
	  
	  while(result != null){
		  System.out.println(result._docid);
		  result = (DocumentIndexed) nextDoc(query, result._docid+1);
	  }
	  
  }

  public void printRuntimeInfo(String msg){
    System.out.println();
    System.out.println(msg);
    System.out.println(new Date());
    int mb = 1024*1024;
    Runtime rt = Runtime.getRuntime();
    System.out.println(rt.freeMemory()/mb);
    System.out.println(rt.totalMemory()/mb);
    System.out.println(rt.maxMemory()/mb);
    System.out.println("used " + (rt.totalMemory() - rt.freeMemory())/mb + "MB");
  }
  
  public static void main(String args[]){
    try {
      Options options = new Options("conf/engine.conf");
      IndexerInvertedDoconly a = new IndexerInvertedDoconly(options);
      //a.constructIndex();
      a.loadIndex();
      /*
      //QueryPhrase q11 = new QueryPhrase("new york");
      QueryPhrase q11 = new QueryPhrase("new york city");
      QueryPhrase q12 = new QueryPhrase("new york film");
      QueryPhrase q13 = new QueryPhrase("bertagna");
      DocumentIndexed d11 = (DocumentIndexed) a.nextDoc(q11, 105);
      System.out.println(d11._docid);
      DocumentIndexed d12 = (DocumentIndexed) a.nextDoc(q12, 126);
      System.out.println(d12._docid);
      DocumentIndexed d13 = (DocumentIndexed) a.nextDoc(q13, 1000);
      System.out.println(d13._docid);
      //a.Docall(q11);
      //a.Docall(q12);
      //a.Docall(q13);
      /*
      QueryPhrase q11 = new QueryPhrase("kicktin");
      DocumentIndexed d11 = (DocumentIndexed) a.nextDoc(q11, -1);
      System.out.println(d11._docid);
      //System.out.println(a.nextDocByTerm("kicktin", -1));
      */
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }  
}