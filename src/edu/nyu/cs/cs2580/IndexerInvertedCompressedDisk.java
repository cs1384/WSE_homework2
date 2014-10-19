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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressedDisk extends Indexer implements Serializable
{
    //indexing result
    
    //private Map<String, Vector<Integer>> _index = new HashMap<String, Vector<Integer>>();
    //private Map<String, ArrayList<Integer>> _index = new HashMap<String, ArrayList<Integer>>();
    
//private Map<String, ArrayList<Byte>> _indexTemp = new HashMap<String, ArrayList<Byte>>();
    //private Map<String, ByteArray> _indexTemp = new HashMap<String, ByteArray>();
    private Map<String, int[]> _indexInts = new HashMap<String, int[]>();
    private Map<String, byte[]> _index = new HashMap<String, byte[]>();
    //Frequency of each term in entire corpus
    private Map<String, Integer> _termCorpusFrequency = new HashMap<String, Integer>();
    //map url to docid to support documentTermFrequency method
    private Map<String, Integer> _urlToDoc = new HashMap<String, Integer>();
    //to store and quick access to basic document information such as title 
    private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();

    private StopWords stopWords;
    private static final long serialVersionUID = 1077111905740085031L;

    
    public IndexerInvertedCompressedDisk()
    {
    }

    public IndexerInvertedCompressedDisk(Options options)
    {
        super(options);
        System.out.println("Using Indexer: " + this.getClass().getSimpleName());
    }

    private void constructPartialIndex(int id, List<File> listOfFiles)
    {
        //Map<String, Vector<Integer>> _indexTemp = new HashMap<String, Vector<Integer>>();
        Map<String, Vector<Posting>> _indexTemp = new HashMap<String, Vector<Posting>>();
        try
        {
            int count = 0;
            
            for (File file : listOfFiles)
            {
                //System.out.println(file.getName());

                String text = TestParse2.getPlainText(file);

                String title = file.getName().replace('_', ' ');
                text = title + " " + text;
                
                processDocument(text, title, _indexTemp); //process each webpage
                
                count++;
                
                if(count % 100 == 0)
                {
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
        } 
        catch (Exception e)
        {
            e.printStackTrace();
        }

        System.out.println("Partially Indexed " + Integer.toString(_numDocs) + " docs with " + Long.toString(_totalTermFrequency) + " terms.");

        try
        {
            //now write this temp hashmap to a file
            String indexFile = _options._indexPrefix + "/partial_cmpr_corpus_" + id + ".idx";
            System.out.println("Store index to: " + indexFile);
            ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
            writer.writeObject(_indexTemp); //write the entire class into the file
            writer.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();;
        }
    }
    
    @Override
    public void constructIndex() throws IOException
    {
        try
        {
            String corpusFolder = _options._corpusPrefix + "/";
            System.out.println("Construct index from: " + corpusFolder);
            int count = 0;
            File folder = new File(corpusFolder);
            ArrayList<File> fileList = new ArrayList<File>();
            for (final File file : folder.listFiles())
            {
                fileList.add(file);
            }
            
            int lower=0, upper = 100;
            int id = 0;
            for(id=0;lower < fileList.size();id++)
            {
                System.out.println("1.  low = " + lower + " , upper = " + upper);
                if(upper > fileList.size())
                    upper = fileList.size();
                System.out.println("2.  low = " + lower + " , upper = " + upper);
                constructPartialIndex(id, fileList.subList(lower, upper));
                lower = upper;
                upper += 100;
            }
        } 
        catch (Exception e)
        {
            e.printStackTrace();
        }

        System.out.println(
                "Indexed " + Integer.toString(_numDocs) + " docs with "
                + Long.toString(_totalTermFrequency) + " terms.");

        
    }

    public void processDocument(String content, String title, Map<String, Vector<Posting>> _indexTemp)
    {
        //String title = "";
        

        DocumentIndexed doc = new DocumentIndexed(_documents.size() + 1);
        doc.setTitle(title);
        String text = content;
        //System.out.println(text);
        //System.out.println("title = " + title);
        
        ProcessTerms(text, doc._docid, _indexTemp);

        //assign random number to doc numViews
        int numViews = (int) (Math.random() * 10000);
        doc.setNumViews(numViews);

        String url = "en.wikipedia.org/wiki/" + title;
        doc.setUrl(url);
        _urlToDoc.put(url, doc._docid); //build up urlToDoc map

        _documents.add(doc);
        _numDocs++;

    }

    public void ProcessTerms(String content, int docid, Map<String, Vector<Posting>> _indexTemp)
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
            token = token.toLowerCase();
            
            //if(stopWords.wordInList(token))
            //    continue;
            
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
        for(String term : op.keySet()){
      Posting posting = new Posting(docid);
      posting.offsets = op.get(term);
      if(_indexTemp.containsKey(term)){
        _indexTemp.get(term).add(posting);
      }else{
        Vector<Posting> docTracker = new Vector<Posting>();
        docTracker.add(posting);
        _indexTemp.put(term, docTracker);
      }
    }
    }
    
    public void mergeIndices(int numOfIndices)
    {
        //for(int i=0;)
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
            
            vec.sort(Comparator);
            newMap.put(str, vec);
        }
        return newMap;
    }

    @Override
    public void loadIndex() throws IOException, ClassNotFoundException
    {
        String indexFile = _options._indexPrefix + "/compressed_corpus.idx";
        System.out.println("Load index from: " + indexFile);

        ObjectInputStream reader = new ObjectInputStream(new FileInputStream(indexFile));
        IndexerInvertedCompressedDisk loaded = (IndexerInvertedCompressedDisk) reader.readObject();

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

        //printIndex();
        System.out.println(Integer.toString(_numDocs) + " documents loaded "
                + "with " + Long.toString(_totalTermFrequency) + " terms!");
    }

    @Override
    public Document getDoc(int docid)
    {
        return (docid > _documents.size() || docid <= 0) ? null : _documents.get(docid);
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
            return _index.get(term).length;
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
        
        for (Map.Entry<String, byte[]> entry : _index.entrySet())
        {
            String key = entry.getKey();
            byte vec[] = entry.getValue();

            //System.out.println("key = " + key);
            System.out.print(key + ": ");
            
            //Convert byte array to integers
            ArrayList<Integer> nums = VByteEncoder.decode(vec);
            for (int i = 0; i < nums.size(); i++)
            {
                System.out.print(nums.get(i) + " ");
            }
            System.out.println("");

        }
        
        
    }
    
    /*
    private class ByteArrayNode  implements Serializable
    {
        final int MAX_SIZE = 1000;
        ByteArrayNode next;
        byte array[];
        int size;
        public ByteArrayNode()
        {
            next = null;
            array = new byte[MAX_SIZE];
            size = 0;
        }
        
        public ByteArrayNode add(byte b)
        {
            if(size < MAX_SIZE)
            {
                array[size++] = b;
                return this;
            }
            else
            {
                next = new ByteArrayNode();
                next.add(b);
                return next;
            }
        }
        
        public ByteArrayNode add(ArrayList<Byte> bList)
        {
            int added = 0;
            while(size < MAX_SIZE && added < bList.size())
            {
                array[size++] = bList.get(added++);
            }
            if(added < bList.size())
            {
                next = new ByteArrayNode();
                ArrayList<Byte> rem = new ArrayList<Byte>();
                for(int i=added;i<bList.size();i++)
                    rem.add(bList.get(added++));
                next.add(rem);
                return next;
            }
            else
                return this;
        }
        
        public int count()
        {
            if(next == null)
                return size;
            else
                return size + next.count();
        }
        
        public byte[] toArray()
        {
            return array;
        }
    }
    
    class ByteArray implements Serializable
    {
        private ByteArrayNode firstByteArray;
        private ByteArrayNode lastByteArray;    

        public ByteArray()
        {
            firstByteArray = new ByteArrayNode();
            lastByteArray = firstByteArray;
        }
        
        public void add(byte b)
        {
            lastByteArray = lastByteArray.add(b);
        }
        
        public void add(ArrayList<Byte> bs)
        {
            lastByteArray = lastByteArray.add(bs);
        }
        
        public int count()
        {
            return firstByteArray.count();
        }
    
    }
    */
    
    class ByteArray implements Serializable
    {
        byte array[];
        int totalElems;
        public ByteArray()
        {
            array = new byte[400];
            totalElems = 0;
        }
        
        public void add(byte b)
        {
            if(totalElems % 400 == 0)
            {
                //create new array
                byte temp[] = new byte[totalElems + 400];
                for(int i=0;i<totalElems;i++)
                {
                    temp[i] = array[i];
                }
                array = temp;
            }
            array[totalElems++] = b;
        }
        
        public int size()
        {
            return totalElems;
        }
        
        public byte get(int i)
        {
            return array[i];
        }
        
        
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
    private class Posting implements Serializable
    {
        
        public int did;
        //get occurance by offsets.size()
        public Vector<Integer> offsets = new Vector<Integer>();
        
        public Posting(int did)
        {
            this.did = did;
        }
    }
    
}
