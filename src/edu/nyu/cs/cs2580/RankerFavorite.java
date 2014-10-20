package edu.nyu.cs.cs2580;

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
public class RankerFavorite extends Ranker
{

    public RankerFavorite(Options options, CgiArguments arguments, Indexer indexer)
    {
        super(options, arguments, indexer);
        
        System.out.println("Using Ranker: " + this.getClass().getSimpleName());
        System.out.println("Using Indexer: " + indexer.getClass().getSimpleName());
        
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
                
                ScoredDocument sd = new ScoredDocument(d, 1);
                retrievedDocs.add(sd);
                d = this._indexer.nextDoc(qp, d._docid);
                
            }
            
            Collections.sort(retrievedDocs);
            
            return retrievedDocs;
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
            
        
        return null;
    }
}
