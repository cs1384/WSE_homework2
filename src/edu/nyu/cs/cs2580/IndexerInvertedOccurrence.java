package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer {
  private static final long serialVersionUID = 1077111905740085030L;
  
  private class Posting{
    int did;
    Vector<Integer> offset = new Vector<Integer>();
    public Posting(int did){
      this.did = did;
    }
  }
  
  private Map<String, Vector<Posting>> _index = 
      new HashMap<String, Vector<Posting>>();
  
  private Map<String, Integer> _termCorpusFrequency = 
      new HashMap<String, Integer>();
  
  private Map<String, Integer> _urlToDoc = new HashMap<String, Integer>(); 
  
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();

  //Provided for serialization
  public IndexerInvertedOccurrence() { }
  
  public IndexerInvertedOccurrence(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
    String corpusFile = _options._corpusPrefix + "/corpus.tsv";
    System.out.println("Construct index from: " + corpusFile);
    
    BufferedReader reader = new BufferedReader(new FileReader(corpusFile));
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        processDocument(line);
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
    writer.writeObject(this);
    writer.close();
  }
  
  public void processDocument(String content){
   
    //docid starts from 1
    DocumentIndexed doc = new DocumentIndexed(_documents.size()+1);
    
    Scanner s = new Scanner(content).useDelimiter("\t");
    String title = s.next();
    
    StringBuilder sb = new StringBuilder();
    sb.append(title);
    sb.append(" ");
    sb.append(s.next());
    ProcessTerms(sb.toString(),doc._docid);
    
    int numViews = Integer.parseInt(s.next());
    //String url = s.next();
    //_urlToDoc.put(url, doc._docid);
    s.close();
    
    doc.setTitle(title);
    doc.setNumViews(numViews);
    //doc.setUrl(url);
    _documents.add(doc);
    _numDocs++;

    return;
  }
  
  public void ProcessTerms(String content, int docid){
    Map<String, Vector<Integer>> op = new HashMap<String,Vector<Integer>>();
    int offset = 1; //offset starts from 1
    Scanner s = new Scanner(content);
    while (s.hasNext()) {
      String token = s.next();
      if(op.containsKey(token)){
        op.get(token).add(offset);
      }else{
        Vector<Integer> offsetTracker = new Vector<Integer>();
        offsetTracker.add(offset);
        op.put(token, offsetTracker);
      }
      if(_termCorpusFrequency.containsKey(token)){
        _termCorpusFrequency.put(token, _termCorpusFrequency.get(token)+1);
      }else{
        _termCorpusFrequency.put(token, 1);
      }
    }
    for(String term : op.keySet()){
      Posting posting = new Posting(docid);
      posting.offset = op.get(term);
      if(_index.containsKey(term)){
        _index.get(term).add(posting);
      }else{
        Vector<Posting> docTracker = new Vector<Posting>();
        docTracker.add(posting);
        _index.put(term, docTracker);
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
  public DocumentIndexed nextDoc(Query query, int docid) {
    return null;
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
      Query query = new Query(term);
      DocumentIndexed di = nextDoc(query,did);
      if(di!=null){
        return di.getOccurance();
      }else{
        return 0;
      }
    }else{
      return 0;
    }
  }
}
