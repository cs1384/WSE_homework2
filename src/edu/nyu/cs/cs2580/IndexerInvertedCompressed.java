package edu.nyu.cs.cs2580;

import java.io.IOException;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends Indexer implements Serializable
{
    //indexing result
    //private Map<String, Vector<Integer>> _index = new HashMap<String, Vector<Integer>>();
    private Map<String, Vector<Integer>> _index = new HashMap<String, Vector<Integer>>();
    private Map<String, Integer[]> _indexInts = new HashMap<String, Integer[]>();
    //Frequency of each term in entire corpus
    private Map<String, Integer> _termCorpusFrequency = new HashMap<String, Integer>();
    //map url to docid to support documentTermFrequency method
    private Map<String, Integer> _urlToDoc = new HashMap<String, Integer>();
    //to store and quick access to basic document information such as title 
    private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();

    private static final long serialVersionUID = 1077111905740085031L;

    public IndexerInvertedCompressed()
    {
    }

    public IndexerInvertedCompressed(Options options)
    {
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
            for (final File file : folder.listFiles())
            {
                System.out.println(file.getName());

                String text = TestParse2.getPlainText(file);

                //Doing this so that processDocument() doesn't break
                text = file.getName().replace('_', ' ') + "\t" + text;
                processDocument(text); //process each webpage
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        System.out.println(
                "Indexed " + Integer.toString(_numDocs) + " docs with "
                + Long.toString(_totalTermFrequency) + " terms.");

        //Before writing, create an integer array and then get rid of vector of ints
        
        for(Map.Entry<String, Vector<Integer> > entry : _index.entrySet())
        {
            Integer tempArray[] = new Integer[entry.getValue().size()];
            for(int i=0;i<entry.getValue().size();i++)
                tempArray[i] = entry.getValue().get(i);
            
            _indexInts.put(entry.getKey(), tempArray);
        }
        _index = null;
        
        String indexFile = _options._indexPrefix + "/compressed_corpus.idx";
        System.out.println("Store index to: " + indexFile);
        ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
        writer.writeObject(this); //write the entire class into the file
        writer.close();

    }

    public void processDocument(String content)
    {
        String title = "";
        StringBuilder sb;
        Scanner s = null;
        try
        {
            //docid starts from 1

            s = new Scanner(content).useDelimiter("\t");
            //Scanner s = new Scanner(content); if other format of corpus
            title = s.next();
            //process terms in this doc to index
            sb = new StringBuilder();
            sb.append(title);
            sb.append(" ");
            sb.append(s.next());
            //close scanner
        } catch (Exception e)
        {
            //IF SHI!T HAPPENS, IGNORE DOC AND MOVE ON, FOR NOW...
            return;
        } finally
        {
            if (s != null)
            {
                s.close();
            }
        }

        DocumentIndexed doc = new DocumentIndexed(_documents.size() + 1);
        doc.setTitle(title);
        String text = sb.toString();
        //System.out.println(text);
        //System.out.println("title = " + title);
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

    public void ProcessTerms(String content, int docid)
    {
        //map for the process of this doc
        Map<String, Vector<Integer>> op = new HashMap<String, Vector<Integer>>();
        int offset = 1; //offset starts from 1
        Scanner s = new Scanner(content);
        while (s.hasNext())
        {
            //put offsets into op map
            String token = s.next();
            //System.out.println(token);
            if (op.containsKey(token))
            {
                op.get(token).add(offset);
            } else
            {
                Vector<Integer> offsetTracker = new Vector<Integer>();
                offsetTracker.add(offset);
                op.put(token, offsetTracker);
            }
            //update the indexer variable
            if (_termCorpusFrequency.containsKey(token))
            {
                _termCorpusFrequency.put(token, _termCorpusFrequency.get(token) + 1);
            } else
            {
                _termCorpusFrequency.put(token, 1);
            }
            offset++;
        }
        s.close();

        //store doc map info into index map 
        for (String term : op.keySet())
        {
            Vector<Integer> intArray = new Vector<Integer>();
            //Posting posting = new Posting(docid);
            
            Vector<Integer> opList = op.get(term);
            int prev = opList.get(0);
            intArray.add(prev);
            for(int i=1;i<opList.size();i++)
            {
                intArray.add(opList.get(i) - prev);
                prev = opList.get(i);
            }
            //posting.offsets = intArray;
                    
            if (_index.containsKey(term))
            {
                _index.get(term).add(docid);
                //_index.get(term).add(posting);
                for(int j=0;j<intArray.size();j++)
                {
                    _index.get(term).add(intArray.get(j));
                }
            } 
            else
            {
                _index.put(term, new Vector<Integer>());
            }
        }
    }

    @Override
    public void loadIndex() throws IOException, ClassNotFoundException
    {
        String indexFile = _options._indexPrefix + "/compressed_corpus.idx";
        System.out.println("Load index from: " + indexFile);

        ObjectInputStream reader = new ObjectInputStream(new FileInputStream(indexFile));
        IndexerInvertedCompressed loaded = (IndexerInvertedCompressed) reader.readObject();

        this._documents = loaded._documents;
        // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
        this._numDocs = _documents.size();
        for (Integer freq : loaded._termCorpusFrequency.values())
        {
            this._totalTermFrequency += freq;
        }

        this._index = loaded._index;
        this._termCorpusFrequency = loaded._termCorpusFrequency;
        this._urlToDoc = loaded._urlToDoc;
        reader.close();

        printIndex();
        System.out.println(Integer.toString(_numDocs) + " documents loaded "
                + "with " + Long.toString(_totalTermFrequency) + " terms!");
    }

    @Override
    public Document getDoc(int docid)
    {
        return (docid > _documents.size() || docid <= 0)
                ? null : _documents.get(docid);
    }

    /**
     * In HW2, you should be using {@link DocumentIndexed}.
     */
    @Override
    public Document nextDoc(QueryPhrase query, int docid)
    {
        int did;
        //keep getting document until no next available 
        while ((did = nextDocByTerms(query._tokens, docid)) != Integer.MAX_VALUE)
        {
            //check if the resulting doc contains all phrases 
            for (Vector<String> phrase : query._phrases)
            {
                //if not, break the for loop and get next doc base on tokens
                if (nextPositionByPhrase(phrase, did, -1) == Integer.MAX_VALUE)
                {
                    break;
                }
            }
            //create return object if passed all phrase test and return
            DocumentIndexed result = new DocumentIndexed(did);
            return result;
        }
        //no possible doc available
        return null;
    }

    public int nextPositionByPhrase(Vector<String> phrase, int docid, int pos)
    {
        int did = nextDocByTerms(phrase, docid - 1);
        if (docid != did)
        {
            return Integer.MAX_VALUE;
        }
        int position = nextPositionByTerm(phrase.get(0), docid, pos);
        boolean returnable = true;
        int largestPos = position;
        int i = 1;
        int tempPos;
        for (; i < phrase.size(); i++)
        {
            tempPos = nextPositionByTerm(phrase.get(i), docid, pos);
            //one of the term will never find next
            if (tempPos == Integer.MAX_VALUE)
            {
                return Integer.MAX_VALUE;
            }
            if (tempPos > largestPos)
            {
                largestPos = tempPos;
            }
            if (tempPos != position + 1)
            {
                returnable = false;
            } else
            {
                position = tempPos;
            }
        }
        if (returnable)
        {
            return position;
        } else
        {
            return nextPositionByPhrase(phrase, docid, largestPos);
        }

    }

    public int nextPositionByTerm(String term, int docid, int pos)
    {
        /*
        if (_index.containsKey(term))
        {
            Vector<Posting> list = _index.get(term);
            Posting op = binarySearchPosting(list, 0, list.size() - 1, docid);
            if (op == null)
            {
                return Integer.MAX_VALUE;
            }
            int largest = op.offsets.lastElement();
            if (largest < pos)
            {
                return Integer.MAX_VALUE;
            }
            if (op.offsets.firstElement() > pos)
            {
                return op.offsets.firstElement();
            }
            return binarySearchOffset(op.offsets, 0, op.offsets.size(), pos);
        }
                */
        return Integer.MAX_VALUE;
    }

    public int binarySearchOffset(Vector<Integer> offsets, int low, int high, int pos)
    {
        int mid;
        while ((high - low) > 1)
        {
            mid = (low + high) / 2;
            if (offsets.get(mid) <= pos)
            {
                low = mid;
            } else
            {
                high = mid;
            }
        }
        return offsets.get(high);
    }
    /*
    public Posting binarySearchPosting(
            Vector<Posting> list, int low, int high, int docid)
    {
        int mid;
        while ((high - low) > 1)
        {
            mid = (low + high) / 2;
            if (list.get(mid).did <= docid)
            {
                low = mid;
            } else
            {
                high = mid;
            }
        }
        if (list.get(high).did == docid)
        {
            return list.get(high);
        } else
        {
            return null;
        }
    }
*/
    public int nextDocByTerms(Vector<String> terms, int curDid)
    {
        if (terms.size() <= 0)
        {
            return curDid;
        }
        int did = nextDocByTerm(terms.get(0), curDid);
        boolean returnable = true;
        int largestDid = did;
        int i = 1;
        int tempDid;
        for (; i < terms.size(); i++)
        {
            tempDid = nextDocByTerm(terms.get(i), curDid);
            //one of the term will never find next
            if (tempDid == Integer.MAX_VALUE)
            {
                return Integer.MAX_VALUE;
            }
            if (tempDid > largestDid)
            {
                largestDid = tempDid;
            }
            if (tempDid != did)
            {
                returnable = false;
            }
        }
        if (returnable)
        {
            return did;
        } else
        {
            return nextDocByTerms(terms, largestDid - 1);
        }
    }

    public int nextDocByTerm(String term, int curDid)
    {
        /*
        if (_index.containsKey(term))
        {
            Vector<Posting> op = _index.get(term);
            int largest = op.lastElement().did;
            if (largest < curDid)
            {
                return Integer.MAX_VALUE;
            }
            if (op.firstElement().did > curDid)
            {
                return op.firstElement().did;
            }
            return binarySearchDoc(op, 0, op.size() - 1, curDid);
        }
        */
        return Integer.MAX_VALUE;
    }
    /*
    public int binarySearchDoc(Vector<Posting> op, int low, int high, int curDid)
    {
        int mid;
        while ((high - low) > 1)
        {
            mid = (low + high) / 2;
            if (op.get(mid).did <= curDid)
            {
                low = mid;
            } else
            {
                high = mid;
            }
        }
        return op.get(high).did;
    }
    */
    @Override
    public int corpusDocFrequencyByTerm(String term)
    {
        if (_index.containsKey(term))
        {
            return _index.get(term).size();
        } else
        {
            return 0;
        }
    }

    @Override
    public int corpusTermFrequency(String term)
    {
        if (_termCorpusFrequency.containsKey(term))
        {
            return _termCorpusFrequency.get(term);
        } else
        {
            return 0;
        }
    }

    @Override
    public int documentTermFrequency(String term, String url)
    {
        if (_urlToDoc.containsKey(url))
        {
            int did = _urlToDoc.get(url);
            QueryPhrase query = new QueryPhrase(term);
            DocumentIndexed di = (DocumentIndexed) nextDoc(query, did);
            if (di != null)
            {
                return di.getOccurance();
            } else
            {
                return 0;
            }
        } else
        {
            return 0;
        }
    }

    public void printIndex()
    {
        /*
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
        */
        
        //Map<String, Vector<Posting>> _index
        for (Map.Entry<String, Vector<Integer>> entry : _index.entrySet())
        {
            String key = entry.getKey();
            Vector<Integer> vec = entry.getValue();

            //System.out.println("key = " + key);
            System.out.print(key + ": ");
            
            for (int i = 0; i < vec.size(); i++)
            {
                System.out.print(vec.get(i));
            }
            System.out.println("");

        }
        
    }
}
