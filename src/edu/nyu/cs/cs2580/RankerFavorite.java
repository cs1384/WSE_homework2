package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
    
    if(!(indexer instanceof IndexerInvertedCompressed))
    {
        System.out.println("Sorry, the favorite ranker can only work with 'Compressed Index'");
        return;
    }
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
        //Get documents which have this term
        
        try
        {
            QueryPhrase qp = new QueryPhrase(query._query);
            
            
            Vector<ScoredDocument> retrievedDocs = new Vector<ScoredDocument>();
            
            if(qp._tokens.size() > 0)
            {
                Document d = this._indexer.nextDoc(qp, 0);
                while(d != null)
                {
                    ScoredDocument sd = scoreDocument(qp, d._docid);

                    retrievedDocs.add(sd);
                    d = this._indexer.nextDoc(qp, d._docid);

                }
            }
            //System.out.println("Got token docs");
            //retrieve phrases
            Set<DocumentIndexed> finalSet = new HashSet<DocumentIndexed>();
            for(int i=0;i<qp._phrases.size();i++)
            {
                try
                {
                    Vector<DocumentIndexed> vec = ((IndexerInvertedCompressed) _indexer).allDocPhrase(qp._phrases.get(i));
                    if(i > 0)
                        finalSet.retainAll(vec);
                    else
                        finalSet.addAll(vec);
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                    System.out.println("Please use compressed index");
                }
            }
            
            for(DocumentIndexed d : finalSet)
            {
                ScoredDocument sd = scoreDocument(qp, d._docid);
                retrievedDocs.add(sd);
            }
            
            System.out.println("Got all docs");
            
            
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
          *freq
          / ((DocumentIndexed)doc).getSize() 
          + lambda
          * _indexer.corpusTermFrequency(str)
          /_indexer._totalTermFrequency);
    }
    score = Math.pow(10,score);
    return new ScoredDocument(doc, score);
  }
  
}
