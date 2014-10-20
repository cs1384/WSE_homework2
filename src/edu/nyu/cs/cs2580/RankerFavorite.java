package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2 based on a refactoring of your favorite
 * Ranker (except RankerPhrase) from HW1. The new Ranker should no longer rely
 * on the instructors' {@link IndexerFullScan}, instead it should use one of
 * your more efficient implementations.
 */
public class RankerFavorite extends Ranker {
  private double lambda = 0.5;
  
  public RankerFavorite(Options options,
      CgiArguments arguments, Indexer indexer) {
    super(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }
  
  public void setLambda(double lambda){
    this.lambda = lambda;
  }
  
  public double getLambda(){
    return this.lambda;
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {
    Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
    DocumentIndexed doc = null;
    int docid = -1;
    while ((doc = (DocumentIndexed)_indexer.nextDoc(query, docid)) != null) {
      rankQueue.add(scoreDocument(query, doc._docid));
      if (rankQueue.size() > numResults) {
        rankQueue.poll();
      }
      docid = doc._docid;
    }
    
    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
    ScoredDocument scoredDoc = null;
    while ((scoredDoc = rankQueue.poll()) != null) {
      results.add(scoredDoc);
    }
    Collections.sort(results, Collections.reverseOrder());
    return results;
    
  }
  
  private ScoredDocument scoreDocument(Query query, int did) {
    // Process the raw query into tokens.
    ((QueryPhrase)query).processQuery();
    // Get the document tokens.
    DocumentIndexed doc = (DocumentIndexed) _indexer.getDoc(did);
    double score = 0.0;
    
    for(int i = 0;i<((QueryPhrase)query)._tokens.size();i++){
      String str = ((QueryPhrase)query)._tokens.get(i);
      score += Math.log10(
          (1-lambda)
          *_indexer.documentTermFrequency(str, doc.getUrl())
          / ((DocumentIndexed)doc).getSize() 
          + lambda
          * _indexer.corpusTermFrequency(str)
          /_indexer._totalTermFrequency);
    }
    for(int i = 0;i<((QueryPhrase)query)._phrases.size();i++){
      for(int j = 0;j<((QueryPhrase)query)._phrases.get(i).size();j++){
      String str = ((QueryPhrase)query)._phrases.get(i).get(j);
      score += Math.log10(
          (1-lambda)
          *_indexer.documentTermFrequency(str, doc.getUrl())
          / ((DocumentIndexed)doc).getSize() 
          + lambda
          * _indexer.corpusTermFrequency(str)
          /_indexer._totalTermFrequency);
      }
    }
    score = Math.pow(10,score);
    return new ScoredDocument(doc, score);
  }
  
}
