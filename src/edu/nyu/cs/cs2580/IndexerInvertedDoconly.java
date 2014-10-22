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
  
  
  
  @Override
  public void constructIndex() throws IOException
  {
    printRuntimeInfo("======= GO WSE_homework2!! =======");
    /*
    File f1 = new File(_options._indexPrefix + "/index0new.txt");
    File f2 = new File(_options._indexPrefix + "/index1new.txt");
    File f3 = new File(_options._indexPrefix + "/index2new.txt");
    f1.createNewFile();
    f2.createNewFile();
    f3.createNewFile();
    */
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
      mergeAllFiles(takeTurn);
      //finalConstruction(takeTurn);
      BufferedReader br = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_" + takeTurn + ".txt")));
      String line = br.readLine();
      
      _op.clear();
      
      int i, op;
      while(line!=null){
    	Vector<Posting> freq = new Vector<Posting>();
        String[] list = line.split(" ");
        //System.out.println(list[0]);
        for(i=1;i<list.length;i++){
          //System.out.println(list[i]);
          Posting newp = new Posting(Integer.parseInt(list[i]));
          freq.add(newp);
        }
        _op.put(list[0],freq);
        line = br.readLine();
      }
      br.close();

      
      
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
          //sb.append(" ").append(p.offsets.size());
          //for(Integer offset : p.offsets){
          //  sb.append(" ").append(offset);
          //}
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
      BufferedReader br = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/Doconly_index.txt")));
      String line = br.readLine();
      //int lineN = 1;
      Vector<Integer> freq = new Vector<Integer>();
      int i, op;
      while(line!=null){
    	System.out.println(line);
        String[] list = line.split(" ");
        for(i=1;i<list.length;i++){
          op = Integer.parseInt(list[i]);
          freq.add(op);
        }
        _index.put(list[0],freq);
        line = br.readLine();
      }
      br.close();
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
    
    /*
    String token;
    while (s.hasNext()) {
      //put offsets into op map
      token = s.next();
      if(_op.containsKey(token)){
        if(_op.get(token).lastElement().did==docid){
          _op.get(token).lastElement().offsets.add(offset);
        }else{
          Vector<Integer> offsetTracker = new Vector<Integer>();
          offsetTracker.add(offset);
          Posting posting = new Posting(docid);
          posting.offsets = offsetTracker;
          _op.get(token).add(posting);
        }
      }else{
        Vector<Integer> offsetTracker = new Vector<Integer>();
        offsetTracker.add(offset);
        Posting posting = new Posting(docid);
        posting.offsets = offsetTracker;
        Vector<Posting> list = new Vector<Posting>();
        list.add(posting);
        _op.put(token, list);
      }
      offset++;
      _totalTermFrequency++;
    }
    s.close();
    */
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
    //makeIndex();
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    this._totalTermFrequency = 0;
    int length = 0;
    for (String rc : _op.keySet()) {
    	length = _op.get(rc).size();
    	this._totalTermFrequency += length;
    }
   
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
  public Document nextDoc(Query query, int docid) {
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
    return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    return 0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }
  
  private int NextDocSingle(String query, int current){
	Vector<Posting> postingList = _op.get(query);
	int postingLength = postingList.size();
	int minID = postingList.firstElement().did;
	int maxID = postingList.lastElement().did;
	if (maxID == 0 || current > maxID){
		return 99999999;
	}else if (minID > current){
		return minID;
	}else{
		int i = 0;
		while(postingList.get(i).did < current){
			i++;
		}
		return postingList.get(i).did;
	}
	
	
  }

  /*
  public Document nextDoc(QueryPhrase query, int docid) {
    boolean keep = false;
    int did = docid;
    //keep getting document until no next available 
    while((did = nextDocByTerms(query._tokens,did))!=Integer.MAX_VALUE){
      keep = false;
      //check if the resulting doc contains all phrases 
      for(Vector<String> phrase : query._phrases){
        //if not, break the for loop and get next doc base on tokens
        if(nextPositionByPhrase(phrase,did,-1)==Integer.MAX_VALUE){
          keep = true;
          break;
        }
      }
      if(keep){
        continue;
      }else{
        //create return object if passed all phrase test and return
        DocumentIndexed result = new DocumentIndexed(did);
        return result;
      }
    }
    //no possible doc available
    return null;
  }
  /*
  public int nextPositionByPhrase(Vector<String> phrase, int docid, int pos){
    int did = nextDocByTerms(phrase, docid-1);
    //System.out.println("docid"+docid);
    if(docid != did){
      return Integer.MAX_VALUE;
    }
    int position = nextPositionByTerm(phrase.get(0), docid, pos);
    boolean returnable = true;
    int largestPos = position;
    int i = 1;
    int tempPos;
    for(;i<phrase.size();i++){
      tempPos = nextPositionByTerm(phrase.get(i), docid, pos);
      //one of the term will never find next
      if(tempPos==Integer.MAX_VALUE){
        return Integer.MAX_VALUE;
      }
      if(tempPos>largestPos){
        largestPos = tempPos;
      } 
      if(tempPos!=position+1){
        returnable = false;
      }else{
        position = tempPos;
      }
    }    
    if(returnable){
      return position;
    }else{
      return nextPositionByPhrase(phrase, docid, largestPos);
    }
    
  }
 
  public int nextPositionByTerm(String term, int docid, int pos){
    Vector<Posting> list = this.getPostingList(term);
    //System.out.println("size"+list.size());
    if(list.size()>0){
      Posting op = binarySearchPosting(list, 0, list.size()-1, docid);
      
      if(op==null){
        return Integer.MAX_VALUE; 
      }
      int largest = op.offsets.lastElement();
      if(largest <= pos){
        return Integer.MAX_VALUE;
      }
      if(op.offsets.firstElement() > pos){
        return op.offsets.firstElement();
      }
      return binarySearchOffset(op.offsets,0,op.offsets.size(),pos);
    }
    return Integer.MAX_VALUE;
  }

  public int binarySearchOffset(Vector<Integer> offsets, int low, int high, int pos){
    int mid;
    while((high-low)>1){
      mid = (low+high)/2;
      if(offsets.get(mid) <= pos){
        low = mid;
      }else{
        high = mid;
      }
    }
    return offsets.get(high);
  }
  
  public Posting binarySearchPosting(
      Vector<Posting> list, int low, int high, int docid){
    int mid;
    while((high-low)>1){
      mid = (low+high)/2;
      if(list.get(mid).did < docid){
        low = mid;
      }else{
        high = mid;
      }
    }
    if(list.get(high).did==docid){
      return list.get(high);
    }else{
      return list.get(low);
    }
  }
  
  public int nextDocByTerms(Vector<String> terms, int curDid){
    if(terms.size()<=0){
      if(curDid<=0){
        return 1;
      }else if(curDid>=_numDocs){
        return Integer.MAX_VALUE;
      }else{
        return curDid+1;
      }
    }
    //System.out.println("term" + curDid);
    int did = nextDocByTerm(terms.get(0), curDid);
    boolean returnable = true;
    int largestDid = did;
    int i = 1;
    int tempDid;
    for(;i<terms.size();i++){
      tempDid = nextDocByTerm(terms.get(i), curDid);
      //one of the term will never find next
      if(tempDid==Integer.MAX_VALUE){
        return Integer.MAX_VALUE;
      }
      if(tempDid>largestDid){
        largestDid = tempDid;
      } 
      if(tempDid!=did){
        returnable = false;
      }
    }    
    if(returnable){
      return did;
    }else{
      return nextDocByTerms(terms, largestDid-1);
    }
  }
  
  public int nextDocByTerm(String term, int curDid){
    Vector<Posting> op = this.getPostingList(term);
    if(op.size()>0){
      int largest = op.lastElement().did;
      //System.out.println("largest"+largest);
      if(largest <= curDid){
        return Integer.MAX_VALUE;
      }
      if(op.firstElement().did > curDid){
        return op.firstElement().did;
      }
      return binarySearchDoc(op,0,op.size()-1,curDid);
    }else
      return Integer.MAX_VALUE;
  }
  
  public int binarySearchDoc(Vector<Posting> op, int low, int high, int curDid){
    int mid;
    while((high-low)>1){
      mid = (low+high)/2;
      if(op.get(mid).did <= curDid){
        low = mid;
      }else{
        high = mid;
      }
    }
    return op.get(high).did;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    Vector<Posting> temp = getPostingList(term);
    return temp.size();
  }
  
  public Vector<Posting> getPostingList(String term){
    Vector<Posting> result = new Vector<Posting>(); 
    if(!_index.containsKey(term)){
      return result;
    }else{
      int lineN = _index.get(term).lineN;
      //System.out.println("lineN:" + lineN);
      try {
        File file = new File(_options._indexPrefix + "/Occurance_index.txt");
        String line = FileUtils.readLines(file).get(lineN-1).toString();
        //System.out.println("line:" + line);
        String[] tokens = line.split(" ");
        int i,top,offsetN;
        for(i = 1;i<tokens.length;){
          Posting posting = new Posting(Integer.parseInt(tokens[i++]));
          offsetN = Integer.parseInt(tokens[i++]);
          posting.offsets = new Vector<Integer>(offsetN);
          top = i+offsetN;
          while(i<top){
            posting.offsets.add(Integer.parseInt(tokens[i++]));
          }
          result.add(posting);
        }
      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return result;
    }
  }

  @Override
  public int corpusTermFrequency(String term) {
    int result = 0;
    Vector<Posting> op = getPostingList(term);
    for(Posting ps : op){
      result += ps.offsets.size();
    }
    return result;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    
    String[] temp = url.split("/");
    String key = temp[temp.length-1];
    if(_urlToDoc.containsKey(key)){
      int did = _urlToDoc.get(key);
      QueryPhrase query = new QueryPhrase(term);
      DocumentIndexed di = (DocumentIndexed)nextDoc(query,did-1);
      if(di!=null){
        return di.getOccurance();
      }else{
        return 0;
      }
    }
    return 0;
  }
*/  
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
    /*
    File file = new File("/Users/Tin/Desktop/test.txt");
    try {
      String line = FileUtils.readLines(file).get(2).toString();
      System.out.println("|" + line + "|");
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    */
    
    
    try {
      Options options = new Options("conf/engine.conf");
      IndexerInvertedDoconly a = new IndexerInvertedDoconly(options);
      //a.constructIndex();
      a.loadIndex();
      //mergeAllFiles(1);
      
      //QueryPhrase q11 = new QueryPhrase("new york");
      QueryPhrase q11 = new QueryPhrase("new york city");
      QueryPhrase q12 = new QueryPhrase("new york film");
      QueryPhrase q13 = new QueryPhrase("bertagna");
      DocumentIndexed d11 = (DocumentIndexed) a.nextDoc(q11, 100);
      System.out.println(d11._docid);
      DocumentIndexed d12 = (DocumentIndexed) a.nextDoc(q12, 100);
      System.out.println(d12._docid);
      DocumentIndexed d13 = (DocumentIndexed) a.nextDoc(q13, 100);
      System.out.println(d13._docid);
      
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