package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import edu.nyu.cs.cs2580.SearchEngine.Options;

import java.io.File;
import java.io.Serializable;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer implements Serializable {
  private static final long serialVersionUID = 1077111905740085032L;
  
  private class Posting implements Serializable{
    public int did;
    //get occurance by offsets.size()
    public Vector<Integer> offsets = new Vector<Integer>();
    public Posting(int did){
      this.did = did;
    }
  }
  private Map<String, Integer> _dictionary =
      new HashMap<String, Integer>();
  //indexing result
  private Map<Integer, Vector<Posting>> _index = 
      new HashMap<Integer, Vector<Posting>>();
  //Frequency of each term in entire corpus
  private Map<Integer, Integer> _termCorpusFrequency = 
      new HashMap<Integer, Integer>();
  //map url to docid to support documentTermFrequency method
  private Map<String, Integer> _urlToDoc = new HashMap<String, Integer>(); 
  //to store and quick access to basic document information such as title 
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  private int _uniqueTerms = 0;
  
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
      try
      {
          String corpusFolder = _options._corpusPrefix + "/";
          System.out.println("Construct index from: " + corpusFolder);

          File folder = new File(corpusFolder);
          
          ArrayList<File> fileList = new ArrayList<File>();
          for (final File file : folder.listFiles()){
              fileList.add(file);
          }
          
          int lower=0, upper = 100;
          int id = 0;
          for(id=0;lower < fileList.size();id++){
              System.out.println("range:  low = " + lower + " , upper = " + upper);
              if(upper > fileList.size())
                  upper = fileList.size();
              constructPartialIndex(id, fileList.subList(lower, upper));
              lower = upper;
              upper += 100;
          }
      } 
      catch (Exception e)
      {
          e.printStackTrace();
      }
      
      
    /*
    try
    {
        String corpusFolder = _options._corpusPrefix + "/";
        System.out.println("Construct index from: " + corpusFolder);

        File folder = new File(corpusFolder);
        for (final File file : folder.listFiles()){
            System.out.println(file.getName());
            String text = TestParse2.getPlainText(file);
  
            //Doing this so that processDocument() doesn't break
            text = file.getName().replace('_', ' ') + "\t" + text;
            processDocument(text); //process each webpage
        }
    } catch (Exception e){
          e.printStackTrace();
    }
    */

      System.out.println(
              "Indexed " + Integer.toString(_numDocs) + " docs with "
              + Long.toString(_totalTermFrequency) + " terms.");
      System.out.println(_uniqueTerms);

      /*
      String indexFile = _options._indexPrefix + "/corpus.idx";
      System.out.println("Store index to: " + indexFile);
      ObjectOutputStream writer
              = new ObjectOutputStream(new FileOutputStream(indexFile));
      writer.writeObject(this); //write the entire class into the file
      writer.close();
      */

  }
  
  private void constructPartialIndex(int id, List<File> listOfFiles){
    //Map<String, Vector<Integer>> _indexTemp = new HashMap<String, Vector<Integer>>();
    Map<Integer, Vector<Posting>> _indexTemp = new HashMap<Integer, Vector<Posting>>();
    try{
      int count = 0;
      for (File file : listOfFiles){
        //System.out.println(file.getName());
        String text = TestParse2.getPlainText(file);
        String title = file.getName().replace('_', ' ');
        text = title + " " + text;
        processDocument(text,title, _indexTemp); //process each webpage
        count++;
              
        if(count % 100 == 0){
          System.out.println("Processed " + count + " documents");
          int mb = 1024*1024;
          //Getting the runtime reference from system
          Runtime runtime = Runtime.getRuntime();
          System.out.println("##### Heap utilization statistics [MB] #####");
          System.out.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / mb);
          System.out.println("Free Memory:" + runtime.freeMemory() / mb);
          System.out.println("Total Memory:" + runtime.totalMemory() / mb);
          System.out.println("Max Memory:" + runtime.maxMemory() / mb);
        }
      }
    }catch(Exception e){
      e.printStackTrace();
    }

    System.out.println("Partially Indexed " + Integer.toString(_numDocs) + " docs with " + Long.toString(_totalTermFrequency) + " terms.");

    try{
      //now write this temp hashmap to a file
      String indexFile = _options._indexPrefix + "/partial_cmpr_corpus_" + id + ".idx";
      System.out.println("Store index to: " + indexFile);
      ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
      writer.writeObject(_indexTemp); //write the entire class into the file
      writer.close();
    }catch(Exception e){
          e.printStackTrace();;
    }
  }
    
  public Map<String, Vector<Posting>> mergeIndices(Map<String, Vector<Posting>> map1, Map<String, Vector<Posting>> map2)
  {
      Set<String> union = new HashSet<String>(map1.keySet());
      union.addAll(map2.keySet());
      
      Map<String, Vector<Posting>> newMap = new HashMap<String, Vector<Posting>>();
      
      for(String str : union)
      {
          Vector<Posting> vec = new Vector<Posting>();
          if(map1.containsKey(str))
              vec.addAll(map1.get(str));
          if(map2.containsKey(str))
              vec.addAll(map2.get(str));
          
          //vec.sort(Comparator);
          newMap.put(str, vec);
      }
      return newMap;
  }
  
  /*
  public void constructIndex() throws IOException {
    String corpusFile = _options._corpusPrefix + "/corpus.tsv";
    System.out.println("Construct index from: " + corpusFile);
    
    BufferedReader reader = new BufferedReader(new FileReader(corpusFile));
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        processDocument(line); //process each webpage
      }
    } finally {
      reader.close();
    }
    
    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with " +
        Long.toString(_totalTermFrequency) + " terms.");
    
    String indexFile = _options._indexPrefix + "/corpus.idx";
    System.out.println("Store index to: " + indexFile);
    ObjectOutputStream writer =
        new ObjectOutputStream(new FileOutputStream(indexFile));
    writer.writeObject(this); //write the entire class into the file
    writer.close();
  }
  */
  /*
  public void processDocument(String content){
    //docid starts from 1
    DocumentIndexed doc = new DocumentIndexed(_documents.size()+1);
    
    Scanner s = new Scanner(content).useDelimiter("\t");
    //Scanner s = new Scanner(content); if other format of corpus
    String title = s.next();
    //process terms in this doc to index
    StringBuilder sb = new StringBuilder();
    sb.append(title);
    sb.append(" ");
    sb.append(s.next());
    ProcessTerms(sb.toString(),doc._docid);
    //close scanner
    s.close();
    
    doc.setTitle(title);
    //assign random number to doc numViews
    int numViews = (int)(Math.random()*10000);
    doc.setNumViews(numViews);

    String url = "en.wikipedia.org/wiki/" + title;
    doc.setUrl(url);
    _urlToDoc.put(url, doc._docid); //build up urlToDoc map
    
    _documents.add(doc);
    _numDocs++;
    return;
  }
  */
  
  public void processDocument(String content){
        String title = "";
        StringBuilder sb;
        Scanner s = null;
        try{
            s = new Scanner(content).useDelimiter("\t");
            //Scanner s = new Scanner(content); if other format of corpus
            title = s.next();
            //process terms in this doc to index
            sb = new StringBuilder();
            sb.append(title);
            sb.append(" ");
            sb.append(s.next());
            //close scanner
        } catch (Exception e){
            //IF SHI!T HAPPENS, IGNORE DOC AND MOVE ON, FOR NOW...
            return;
        } finally{
            if (s != null){
                s.close();
            }
        }
        //docid starts from 1
        DocumentIndexed doc = new DocumentIndexed(_documents.size() + 1);
        doc.setTitle(title);
        String text = sb.toString();
        ProcessTerms(text, doc._docid);
        //assign random number to doc numViews
        int numViews = (int) (Math.random() * 10000);
        doc.setNumViews(numViews);

        String url = "en.wikipedia.org/wiki/" + title;
        doc.setUrl(url);
        _urlToDoc.put(url, doc._docid); //build up urlToDoc map

        _documents.add(doc);
        _numDocs++;
        return;
    }
  
  public void processDocument(String content,String title, Map<Integer, Vector<Posting>> _indexTemp){
    
    //docid starts from 1
    DocumentIndexed doc = new DocumentIndexed(_documents.size() + 1);
    doc.setTitle(title);
    ProcessTerms(content, doc._docid, _indexTemp);
    //assign random number to doc numViews
    int numViews = (int) (Math.random() * 10000);
    doc.setNumViews(numViews);

    String url = "en.wikipedia.org/wiki/" + title;
    doc.setUrl(url);
    _urlToDoc.put(url, doc._docid); //build up urlToDoc map

    _documents.add(doc);
    _numDocs++;
    return;
  }
  
  public void ProcessTerms(String content, int docid, Map<Integer, Vector<Posting>> _indexTemp){
    //map for the process of this doc
    Map<String, Vector<Integer>> op = new HashMap<String,Vector<Integer>>();
    int offset = 1; //offset starts from 1
    Scanner s = new Scanner(content);
    String token;
    int termN;
    while (s.hasNext()) {
      //put offsets into op map
      token = s.next();
      if(op.containsKey(token)){
        op.get(token).add(offset);
      }else{
        Vector<Integer> offsetTracker = new Vector<Integer>();
        offsetTracker.add(offset);
        op.put(token, offsetTracker);
      }
      if(_dictionary.containsKey(token)){
        termN = _dictionary.get(token);
      }else{
        _dictionary.put(token, ++_uniqueTerms);
        termN = _uniqueTerms;
      }
      //update the indexer variable
      if(_termCorpusFrequency.containsKey(termN)){
        _termCorpusFrequency.put(termN, _termCorpusFrequency.get(termN)+1);
      }else{
        _termCorpusFrequency.put(termN, 1);
      }
      offset++;
      _totalTermFrequency++;
    }
    s.close();
    //store doc map info into index map 
    for(String term : op.keySet()){
      termN = _dictionary.get(term);
      Posting posting = new Posting(docid);
      posting.offsets = op.get(term);
      if(_indexTemp.containsKey(termN)){
        _indexTemp.get(termN).add(posting);
      }else{
        Vector<Posting> docTracker = new Vector<Posting>();
        docTracker.add(posting);
        _indexTemp.put(termN, docTracker);
      }
    }
  }

  
  public void ProcessTerms(String content, int docid){
    //map for the process of this doc
    Map<String, Vector<Integer>> op = new HashMap<String,Vector<Integer>>();
    int offset = 1; //offset starts from 1
    Scanner s = new Scanner(content);
    String token;
    int termN;
    while (s.hasNext()) {
      //put offsets into op map
      token = s.next();
      if(op.containsKey(token)){
        op.get(token).add(offset);
      }else{
        Vector<Integer> offsetTracker = new Vector<Integer>();
        offsetTracker.add(offset);
        op.put(token, offsetTracker);
      }
      if(_dictionary.containsKey(token)){
        termN = _dictionary.get(token);
      }else{
        _dictionary.put(token, ++_uniqueTerms);
        termN = _uniqueTerms;
      }
      //update the indexer variable
      if(_termCorpusFrequency.containsKey(termN)){
        _termCorpusFrequency.put(termN, _termCorpusFrequency.get(termN)+1);
      }else{
        _termCorpusFrequency.put(termN, 1);
      }
      offset++;
      _totalTermFrequency++;
    }
    s.close();
    //store doc map info into index map 
    for(String term : op.keySet()){
      termN = _dictionary.get(term);
      Posting posting = new Posting(docid);
      posting.offsets = op.get(term);
      if(_index.containsKey(termN)){
        _index.get(termN).add(posting);
      }else{
        Vector<Posting> docTracker = new Vector<Posting>();
        docTracker.add(posting);
        _index.put(termN, docTracker);
      }
    }
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    String indexFile = _options._indexPrefix + "/corpus.idx";
    System.out.println("Load index from: " + indexFile);

    ObjectInputStream reader =
        new ObjectInputStream(new FileInputStream(indexFile));
    IndexerInvertedOccurrence loaded = 
        (IndexerInvertedOccurrence) reader.readObject();

    this._documents = loaded._documents;
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    for (Integer freq : loaded._termCorpusFrequency.values()) {
      this._totalTermFrequency += freq;
    }
   
    this._index = loaded._index;
    this._termCorpusFrequency = loaded._termCorpusFrequency;
    this._urlToDoc = loaded._urlToDoc;
    reader.close();

    //printIndex();
    
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
    int termN = _dictionary.get(term);
    if(_index.containsKey(termN)){
      Vector<Posting> list = _index.get(termN);
      Posting op = binarySearchPosting(list, 0, list.size()-1, docid);
      if(op==null){
        return Integer.MAX_VALUE; 
      }
      int largest = op.offsets.lastElement();
      if(largest < pos){
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
      if(list.get(mid).did <= docid){
        low = mid;
      }else{
        high = mid;
      }
    }
    if(list.get(high).did==docid){
      return list.get(high);
    }else{
      return null;
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
    int termN = _dictionary.get(term);
    if(_index.containsKey(termN)){
      Vector<Posting> op = _index.get(termN);
      int largest = op.lastElement().did;
      if(largest < curDid){
        return Integer.MAX_VALUE;
      }
      if(op.firstElement().did > curDid){
        return op.firstElement().did;
      }
      return binarySearchDoc(op,0,op.size()-1,curDid);
    }
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
    if(_index.containsKey(term)){
      return _index.get(term).size();
    }else{
      return 0;
    }
  }

  @Override
  public int corpusTermFrequency(String term) {
    if(_termCorpusFrequency.containsKey(term)){
      return _termCorpusFrequency.get(term);
    }else{
      return 0;
    }
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    if(_urlToDoc.containsKey(url)){
      int did = _urlToDoc.get(url);
      QueryPhrase query = new QueryPhrase(term);
      DocumentIndexed di = (DocumentIndexed)nextDoc(query,did);
      if(di!=null){
        return di.getOccurance();
      }else{
        return 0;
      }
    }else{
      return 0;
    }
  }
  
  /*
  public void printIndex()
    {
        //Map<String, Vector<Posting>> _index
        for (Map.Entry<String, Vector<Posting>> entry : _index.entrySet())
        {
            String key = entry.getKey();
            Vector<Posting> vec = entry.getValue();

            //System.out.println("key = " + key);
            System.out.print(key + ": ");
            
            for (int i = 0; i < vec.size(); i++)
            {
                //System.out.println("\tDoc id = " + vec.get(i).did);
                System.out.print("(");
                System.out.print(vec.get(i).did + ", ");
                
                Vector<Integer> offsets = vec.get(i).offsets;
                System.out.print(offsets.size() + ", [");

                for (int j = 0; j < offsets.size(); j++)
                {
                    System.out.print(offsets.get(j) + ", ");
                }
                
                System.out.print("]), ");
            }
            System.out.println("");

        }
        
    }
    */
  public static void main(String args[]){
    
    try {
      Options options = new Options("conf/engine.conf");
      IndexerInvertedOccurrence a = new IndexerInvertedOccurrence(options);
      a.constructIndex();
      //a.loadIndex();
      /*
      QueryPhrase q11 = new QueryPhrase("kicktin");
      QueryPhrase q12 = new QueryPhrase("kicktin kickass");
      QueryPhrase q13 = new QueryPhrase("\"kicktin kickass\"");
      DocumentIndexed d11 = (DocumentIndexed) a.nextDoc(q11, -1);
      System.out.println(d11._docid);
      DocumentIndexed d12 = (DocumentIndexed) a.nextDoc(q12, -1);
      System.out.println(d12._docid);
      DocumentIndexed d13 = (DocumentIndexed) a.nextDoc(q13, -1);
      System.out.println(d13._docid);
      */
      /*
      QueryPhrase q11 = new QueryPhrase("Zatanna");
      DocumentIndexed d11 = (DocumentIndexed) a.nextDoc(q11, -1);
      System.out.println(d11._docid);
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
