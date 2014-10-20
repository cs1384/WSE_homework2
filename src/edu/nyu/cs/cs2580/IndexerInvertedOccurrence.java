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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Vector;

import edu.nyu.cs.cs2580.SearchEngine.Options;

import java.io.File;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer implements Serializable {
  private static final long serialVersionUID = 1077111905740085032L;
  
  private class Record implements Serializable{
    int lineN = -1;
    int fre = 0;
    public Record(int l, int f){
      lineN = l;
      fre = f;
    }
  }
  private Map<String, Record> _index =
      new HashMap<String, Record>();
  
  private class Posting implements Serializable{
    public int did;
    //get occurance by offsets.size()
    public Vector<Integer> offsets = new Vector<Integer>();
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
  public IndexerInvertedOccurrence() { }
  //constructor
  public IndexerInvertedOccurrence(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }
  
  @Override
  public void constructIndex() throws IOException
  {
    printRuntimeInfo("======= GO WSE_homework2!! =======");
    
    File f1 = new File(_options._indexPrefix + "/index0.txt");
    File f2 = new File(_options._indexPrefix + "/index1.txt");
    File f3 = new File(_options._indexPrefix + "/index2.txt");
    f1.createNewFile();
    f2.createNewFile();
    f3.createNewFile();
    
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
        mergeFiles(takeTurn); //merge previous output files
        _op.clear();
        lower = upper;
        upper += 150;
        takeTurn = takeTurn + 2;
      } 
      finalConstruction(takeTurn);
      
    }catch (Exception e){
        e.printStackTrace();
    }
    
    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with "
        + Long.toString(_totalTermFrequency) + " terms.");
    //System.out.println(_uniqueTerms);

    String indexFile = _options._indexPrefix + "/OccuranceIndexer.idx";
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
      bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/index" + takeTurn%3 + ".txt")));
      Vector<Posting> pv;
      StringBuilder sb = new StringBuilder(); 
      for(String term : _op.keySet()){
        sb.append(term);
        pv = _op.get(term);
        for(Posting p : pv)
        {
          sb.append(" ").append(p.did);
          sb.append(" ").append(p.offsets.size());
          for(Integer offset : p.offsets){
            sb.append(" ").append(offset);
          }
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
  
  public void mergeFiles(int takeTurn){
    
    try {
      int merge = takeTurn%3 - 1==-1?2:takeTurn%3 - 1;
      int target = takeTurn%3 + 1==3?0:takeTurn%3 + 1;
      
      //System.out.println(merge);
      //System.out.println(takeTurn%3);
      //System.out.println(target);
      
      BufferedReader br1 = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/index" + takeTurn%3 + ".txt")));
      BufferedReader br2 = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/index" + merge + ".txt")));
      BufferedWriter bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/index" + target + ".txt")));
      
      String line1 = br1.readLine();
      String line2 = br2.readLine();
      
      while(line1 != null && line2 != null){
        String tokens1[] = line1.split(" ");
        String tokens2[] = line2.split(" ");
        if(tokens1[0].compareTo(tokens2[0]) < 0){
          if(tokens1[0].equals("kicktin"))
            //System.out.println("kicktin1");
          bw.write(line2 + "\n");
          line2 = br2.readLine();
        }else if(tokens1[0].compareTo(tokens2[0]) > 0){
          if(tokens2[0].equals("kicktin"))
            //System.out.println("kicktin2");
          bw.write(line1 + "\n");
          line1 = br1.readLine();
        }else{
          StringBuilder sb = new StringBuilder();
          sb.append(line2);
          for(int k=1;k<tokens1.length;k++){
            sb.append(" ").append(tokens1[k]);
          }
          line1 = br1.readLine();
          line2 = br2.readLine();
        }
      }
      
      while(line1 != null){
        bw.write(line1 + "\n");
        line1 = br1.readLine();
      }
      
      while(line2 != null){
        bw.write(line2 + "\n");
        line2 = br2.readLine();
      }
      
      bw.close();
      br1.close();
      br2.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void finalConstruction(int takeTurn){
    try{
      BufferedReader br = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/index" + takeTurn%3 + ".txt")));
      BufferedWriter bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/Occurance_index.txt")));
      String line = br.readLine();
      while(line!=null){
        bw.write(line + "\n");
        line = br.readLine();
      }
    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    /*
    File f1 = new File(_options._indexPrefix + "/index0.txt");
    File f2 = new File(_options._indexPrefix + "/index1.txt");
    File f3 = new File(_options._indexPrefix + "/index2.txt");
    f1.delete();
    f2.delete();
    f3.delete();
    */
  }
  
  public void makeIndex(){
    try{
      BufferedReader br = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/Occurance_index.txt")));
      String line = br.readLine();
      int lineN = 1;
      int fre, i, op;
      while(line!=null){
        String[] list = line.split(" ");
        fre = 0;
        for(i=2;i<list.length;){
          op = Integer.parseInt(list[i]);
          fre += op;
          i = i + op + 2;
        }
        _index.put(list[0],new Record(lineN,fre));
        lineN++;
        line = br.readLine();
      }
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
    int offset = 1; //offset starts from 1
    Scanner s = new Scanner(content);
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
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    String indexFile = _options._indexPrefix + "/OccuranceIndexer.idx";
    System.out.println("Load index from: " + indexFile);

    ObjectInputStream reader =
        new ObjectInputStream(new FileInputStream(indexFile));
    IndexerInvertedOccurrence loaded = 
        (IndexerInvertedOccurrence) reader.readObject();

    this._documents = loaded._documents;
    makeIndex();
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    this._totalTermFrequency = 0;
    for (Record rc : _index.values()) {
      this._totalTermFrequency += rc.fre;
    }
   
    this._op = loaded._op;
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
      IndexerInvertedOccurrence a = new IndexerInvertedOccurrence(options);
      //a.constructIndex();
      a.loadIndex();
      
      QueryPhrase q11 = new QueryPhrase("\"Bert Kaempfert\" television");
      //QueryPhrase q12 = new QueryPhrase("\"new york city\" film");
      //QueryPhrase q13 = new QueryPhrase("\"kickass kicktin\"");
      DocumentIndexed d11 = (DocumentIndexed) a.nextDoc(q11, -1);
      System.out.println(d11._docid);
      //DocumentIndexed d12 = (DocumentIndexed) a.nextDoc(q12, -1);
      //System.out.println(d12._docid);
      //DocumentIndexed d13 = (DocumentIndexed) a.nextDoc(q13, -1);
      //System.out.println(d13._docid);
      
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
  public Document nextDoc(Query query, int docid) {
    // TODO Auto-generated method stub
    return null;
  }
}
