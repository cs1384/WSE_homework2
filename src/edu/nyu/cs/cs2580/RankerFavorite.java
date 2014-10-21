package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import java.util.Collections;

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
  public Vector<ScoredDocument> runQuery(Query query, int numResults)
    {
        //System.out.println("SSS");
        
        //Get documents which have this term
        System.out.println("num docs = " + this._indexer._numDocs);
        try
        {
            QueryPhrase qp = new QueryPhrase(query._query);
            //Vector<Document> retrievedDocs = new Vector<Document>();
            Vector<ScoredDocument> retrievedDocs = new Vector<ScoredDocument>();
            
            Document d = this._indexer.nextDoc(qp, 0);
            while(d != null)
            {
                //System.out.println("d = " + d._docid);
                //System.out.println("found in " + d.getUrl());
                
                //ScoredDocument sd = new ScoredDocument(d, 1);
                ScoredDocument sd = scoreDocument(qp, d._docid);
                
                retrievedDocs.add(sd);
                d = this._indexer.nextDoc(qp, d._docid);
                
            }
            
            Collections.sort(retrievedDocs, Collections.reverseOrder());
            
            return retrievedDocs;
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
            
        
        return null;
    }
  
  private ScoredDocument scoreDocument(QueryPhrase query, int did) {

    DocumentIndexed doc = (DocumentIndexed) _indexer.getDoc(did);
    if(doc == null)
          System.out.println("Null doc");
    double score = 0.0;
    
      
    for(int i = 0;i<query._tokens.size();i++)
    {
      String str = query._tokens.get(i);
      
      int freq = _indexer.documentTermFrequency(str, doc.getUrl());
      
      
      score += Math.log10(
          (1-lambda)
          *_indexer.documentTermFrequency(str, doc.getUrl())
          / ((DocumentIndexed)doc).getSize() 
          + lambda
          * _indexer.corpusTermFrequency(str)
          /_indexer._totalTermFrequency);
       
    }
    
    score = Math.pow(10,score);
    return new ScoredDocument(doc, score);
  }
  
}
