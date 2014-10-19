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
import java.util.Arrays;
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
    //private Map<String, ArrayList<Integer>> _index = new HashMap<String, ArrayList<Integer>>();
    
//private Map<String, ArrayList<Byte>> _indexTemp = new HashMap<String, ArrayList<Byte>>();
    
    ////private Map<String, ByteArray> _indexTemp = new HashMap<String, ByteArray>();
    
    //private Map<String, int[]> _indexInts = new HashMap<String, int[]>();
    private Map<String, Integer> posMap = new HashMap<String, Integer>();
    private Vector<String> wordList = new Vector<String>();
    
    //private Vector<ByteArray2> vectoredMap = new Vector<ByteArray2>();
    private Vector<ByteArray> vectoredMap = new Vector<ByteArray>();
    //private Vector<Vector<Byte>> vectoredMap = new Vector<Vector<Byte>>();
    
    //private Map<String, byte[]> _index = new HashMap<String, byte[]>();
    private Vector<byte[]> _index = new Vector<byte[]>();
    
    //private Map<String, ArrayList<Pair> > skipList = new HashMap<String, ArrayList<Pair>>();
    private Vector<ArrayList<Pair> > skipList = new Vector<ArrayList<Pair>>();
    //Frequency of each term in entire corpus
    private Map<String, Integer> _termCorpusFrequency = new HashMap<String, Integer>();
    //map url to docid to support documentTermFrequency method
    private Map<String, Integer> _urlToDoc = new HashMap<String, Integer>();
    //to store and quick access to basic document information such as title 
    private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();

    private StopWords stopWords;
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
            stopWords = new StopWords(_options);
                    
            String corpusFolder = _options._corpusPrefix + "/";
            System.out.println("Construct index from: " + corpusFolder);
            int count = 0;
            File folder = new File(corpusFolder);
            File[] files = folder.listFiles();
            Arrays.sort(files);
            for (final File file : files)
            {
                //System.out.println("file name = " + file.getName());

                String text = TestParse2.getPlainText(file);
                
                
                //Doing this so that processDocument() doesn't break
                //text = file.getName().replace('_', ' ') + "\t" + text;
                String title = file.getName().replace('_', ' ');
                text = title + " " + text;
                
                processDocument(text, title); //process each webpage
                
                count++;
                
                if(count % 100 == 0)
                {
                    System.out.println("Processed " + count + " documents");
                    
                    
                    
                    
                    int mb = 1024*1024;
        /* 
        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
         
        System.out.println("##### Heap utilization statistics [MB] #####");
         
        //Print used memory
        System.out.println("Used Memory:"
            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
 
        //Print free memory
        System.out.println("Free Memory:"
            + runtime.freeMemory() / mb);
         
        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
 
        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
        */
        
        
        
                }
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        System.out.println(
                "Indexed " + Integer.toString(_numDocs) + " docs with "
                + Long.toString(_totalTermFrequency) + " terms.");

        //Before writing, create an integer array and then get rid of vector of ints
        
        
        //for(Map.Entry<String, ByteArray > entry : _indexTemp.entrySet())
        for(int i=0;i<vectoredMap.size();i++)
        {
            
            /*
            int tempArray[] = new int[entry.getValue().size()];
            for(int i=0;i<entry.getValue().size();i++)
                tempArray[i] = entry.getValue().get(i);
            
            _indexInts.put(entry.getKey(), tempArray);
            */
            //System.out.println("key = " + entry.getKey());
            
            //ByteArray byteList = entry.getValue();
            //ByteArray2 byteList = vectoredMap.get(i);
            ByteArray byteList = vectoredMap.get(i);
            //Vector<Byte> byteList = vectoredMap.get(i);
            
            byte tempArray[] = new  byte[byteList.size()];
            for(int j=0;j<byteList.size();j++)
            {
                tempArray[j] = byteList.get(j);
                //System.out.print((int)tempArray[i]);
            }
            //System.out.println("");
            //_indexTemp.put(entry.getKey(), null);
            vectoredMap.set(i, null);
            
            /*
            ArrayList<Integer> intList = VByteEncoder.decode(tempArray);
            ArrayList<Pair> pp = new  ArrayList<Pair>();
            for(int j=0;j<intList.size();j++)
            {
                int docId = intList.get(j);
                int num = intList.get(j+1);
                
                //System.out.println("docId = " + docId);
                
                //pp.add(new Pair(docId, ));
                j += 1 + num;
            }
            */
            //_index.set(i, tempArray);
            _index.add(tempArray);
            
        }
        //_indexTemp = null;
        vectoredMap = null;
        stopWords = null;
        
        String indexFile = _options._indexPrefix + "/compressed_corpus.idx";
        System.out.println("Store index to: " + indexFile);
        ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
        writer.writeObject(this); //write the entire class into the file
        writer.close();

    }

    public void processDocument(String content, String title)
    {
        //String title = "";
        

        DocumentIndexed doc = new DocumentIndexed(_documents.size() + 1);
        doc.setTitle(title);
        String text = content;
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

    }

    public void ProcessTerms(String content, int docid)
    {
        //System.out.println("::: docid = " + docid);
        //map for the process of this doc
        Map<String, Vector<Integer>> op = new HashMap<String, Vector<Integer>>();
        int offset = 1; //offset starts from 1
        Scanner s = new Scanner(content);
        while (s.hasNext())
        {
            //put offsets into op map
            String token = s.next();
            //System.out.println("::::::" + token);
            token = token.toLowerCase();
            
            if(stopWords.wordInList(token))
            {
                //System.out.println("Stop word: " + token);
                continue;
            }
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
            //ArrayList<Integer> intArray = new ArrayList <Integer>();
            
            //Posting posting = new Posting(docid);
            
            Vector<Integer> opList = op.get(term);
              
           
            //if (!_indexTemp.containsKey(term))
            if (!posMap.containsKey(term))
            {
                //_indexTemp.put(term, new ByteArray());
                
                posMap.put(term, vectoredMap.size());
                //vectoredMap.add(new ByteArray2());
                vectoredMap.add(new ByteArray());
                //vectoredMap.add(new Vector<Byte>());
                wordList.add(term);
                ArrayList<Pair> pl = new ArrayList<Pair>();
                pl.add(new Pair(docid, 0));
                skipList.add(new ArrayList<Pair>());
            }
            //question: can we now guess the number of entries using num of docs and freqs??
            
            int vectorPos = posMap.get(term);
            //Posting list contains docId and number of occurences
            //ByteArray termPosting = _indexTemp.get(term);
            
            //ByteArray2 termPosting = vectoredMap.get(vectorPos);
            ByteArray termPosting = vectoredMap.get(vectorPos);
            //Vector<Byte> termPosting = vectoredMap.get(vectorPos);
            
            //System.out.println("term = " + term);
            //System.out.println("Putting doc " + docid + " at byte #" + termPosting.size());
            
            
            //guess if this is necessary
            ArrayList<Pair> pl = skipList.get(vectorPos);
            if(pl.size() > 0)
            {
                Pair p = pl.get(pl.size()-1);
                if(termPosting.size() - p.p > 200)
                {
                    pl.add(new Pair(docid, termPosting.size()));
                }
            }
               
            
            byte bigByteArray[];
            int total = 0;
            byte bArray[] = VByteEncoder.encode(docid);
            
            for(byte b : bArray)
                //termPosting.add(bArray);  
                termPosting.add(b);  

            bArray = VByteEncoder.encode(opList.size());
            for(byte b : bArray)
            //termPosting.add(bArray);  
                termPosting.add(b);  

            
            //Now add offsets to posting list
            int prev = opList.get(0);
            bArray = VByteEncoder.encode(prev);
            for(byte b : bArray)
                termPosting.add(b);  
            //termPosting.add(bArray);  

            for(int j=1;j<opList.size();j++)
            {
                int x = opList.get(j);
                bArray = VByteEncoder.encode(x - prev);
                for(byte b : bArray)
                    termPosting.add(b);  
                //termPosting.add(bArray);  
                    

                prev = x;
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
        this.skipList = loaded.skipList;
        this.posMap = loaded.posMap;
        this.vectoredMap = loaded.vectoredMap;
        this.wordList = loaded.wordList;
        this._termCorpusFrequency = loaded._termCorpusFrequency;
        this._urlToDoc = loaded._urlToDoc;
        reader.close();

        printIndex();
        System.out.println(Integer.toString(_numDocs) + " documents loaded "
                + "with " + Long.toString(_totalTermFrequency) + " terms!");
        
        /*
        ArrayList<Pair> pl = skipList.get("abc");
        if(pl == null)
            System.out.println("DSDS");
        //System.out.println("pl.size() = " + pl.size());
        for(Pair p : pl)
        {
            //System.out.println("p.d = " + p.d + " , p.p = " + p.p);
            nextDocByTerm("abc", p.p);
        }
        */
        //System.out.println("TEST :: ");
        int nd = next("abc", 1);
        //System.out.println("Found next");
        
        
        Vector<String> query = new Vector<String>();
        query.add("abc");
        query.add("def");
        int doc = 0;
        while(true)
        {
            DocumentIndexed d = nextDoc2(query,doc);
            if(d == null)
                break;
            System.out.println("d = " + d._docid);
            doc = d._docid;
        }
        
        
    }

    private int next(String w, int docId)
    {
        int wordIndex = posMap.get(w);
        //System.out.println("next: " + w + ", " + docId);
        //Scan skip list to find this doc
        ArrayList<Pair> pl = skipList.get(wordIndex);
        if(pl == null)
            System.out.println("DSDS");
        
        //System.out.println("pl.size() = " + pl.size());
        int prev = 0;
        int offset = 0;
        for(Pair p : pl)
        {
            prev = offset;
            offset = p.p;
            //System.out.println("p.d = " + p.d + " , p.p = " + p.p);
            
            if(p.d >= docId)
                break;
            //nextDocByTerm("abc", p.p);
        }
        offset = prev;
        byte bList[] = _index.get(wordIndex);
        //System.out.println("A");
        //for(int i=0;i<bList.length;i++)
        int i = 0;
        Integer nextLoc = offset;
        boolean found = false;
        while(i < bList.length)
        {
            int x[] = VByteEncoder.getFirstNum(bList, nextLoc);
            int doc = x[0];
            nextLoc = x[1];
            //System.out.println("doc= " + doc);
            //System.out.println("Next loc= " + nextLoc);
            
            if(doc > docId)
                found = true;
            x = VByteEncoder.getFirstNum(bList, nextLoc);
            int numOccur = x[0];
            nextLoc = x[1];
            
            //System.out.println("numOccur = " + numOccur);

            for(int j=0;j<numOccur;j++)
            {
                x = VByteEncoder.getFirstNum(bList, nextLoc);
                int loc = x[0];
                nextLoc = x[1];
            }
            int docId2 = bList[i];
            int numOffsets = bList[i+1];    //Assume this won't crash
            
            
            //next doc's offset:
            i = nextLoc; //1 because of numOffsets
            
            if(found)
                return doc;
        }
        
        //if(!found)
        //    System.out.println("Not found");
        //now do a linear search to find the doc after docId
        return Integer.MAX_VALUE;
    }
    
    @Override
    public Document getDoc(int docid)
    {
        return (docid > _documents.size() || docid <= 0) ? null : _documents.get(docid);
    }

    public DocumentIndexed nextDoc2(Vector<String> query, int docid)
    {
        //System.out.println("nextDoc2");
        ArrayList<Integer> pos = new ArrayList<Integer>();
        for(int i=0;i<query.size();i++)
        {
            int n = next(query.get(i), docid);
            //System.out.println("for " + query.get(i) + ",  n = " + n + ",   with docid=" + docid);
            
            if(n == Integer.MAX_VALUE)
            {
                //DocumentIndexed result = new DocumentIndexed(Integer.MAX_VALUE);
                return null;
            }
            
            pos.add(n);
        }
        
        boolean mismatch = false;
        for(int i=1;i<query.size();i++)
        {
            if(pos.get(i-1) != pos.get(i))
            {
                mismatch = true;
                break;
            }
        }
        
        if(mismatch)
        {
            //System.out.println("mismatch");
            int max = 0;
            for(int i=0;i<query.size();i++)
            {
                if(pos.get(i) > max)
                    max = pos.get(i);
            }
            
            return nextDoc2(query, max-1);
        }
        
        
        
        return new DocumentIndexed(pos.get(0));
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
        //to test, print all docs this term appears in:
        byte bList[] = _index.get(term);
        
        //for(int i=0;i<bList.length;i++)
        int i = 0;
        Integer nextLoc = 0;
        int num = VByteEncoder.getFirstNum(bList, curDid, nextLoc);
        System.out.println("num = " + num);
            
        while(false && i < bList.length)
        {
            int docId = bList[i];
            int numOffsets = bList[i+1];    //Assume this won't crash
            
            
            //next doc's offset:
            i = i + 1 + numOffsets; //1 because of numOffsets
        }
        */
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
    {/*
        if (_index.containsKey(term))
        {
            return _index.get(term).length;
        } else
        {
            return 0;
        }
        */
        return 0;
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
        
        //for (Map.Entry<String, byte[]> entry : _index.entrySet())
        for(int i=0;i<_index.size();i++)
        {
            String key = wordList.get(i);
            byte vec[] = _index.get(i);

            //System.out.println("key = " + key);
            System.out.print(key + ": ");
            
            //Convert byte array to integers
            ArrayList<Integer> nums = VByteEncoder.decode(vec);
            for (int j = 0; j < nums.size(); j++)
            {
                System.out.print(nums.get(j) + " ");
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
        int SIZE = 512;
        public ByteArray()
        {
            //array = new byte[1000];
            totalElems = 0;
        }
        
        public void add(byte b)
        {
            if(totalElems % SIZE == 0)
            {
                //create new array
                byte temp[] = new byte[totalElems + SIZE];
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
    
    /*
    class ByteArray2 //implements Serializable
    {
        ArrayList<byte[]> arrayOfArray;
        int totalElems;
        public ByteArray2()
        {
            //array = new byte[1000];
            totalElems = 0;
            arrayOfArray = new ArrayList<byte[]>();
        }
        
        public void add(byte[] arr)
        {
            arrayOfArray.add(arr);
            totalElems += arr.length;
        }
        
        public int size()
        {
            return totalElems;
        }
        
        public byte get(int i)
        {
            int j = i;
            int indx = 0;
            while(j >= arrayOfArray.get(indx).length)
            {
                j -= arrayOfArray.get(indx).length;
                indx++;
            }
            return arrayOfArray.get(indx)[j];
        }
        
        
    }
    */
    /*
    class ByteArray implements Serializable
    {
        ArrayList<Byte> array;
        public ByteArray()
        {
            array = new ArrayList<Byte>();
        }
        
        public void add(byte b)
        {
            array.add(b);
        }
        
        public int size()
        {
            return array.size();
        }
        
        public byte get(int i)
        {
            return array.get(i);
        }
        
        
    }
    */
    class Pair implements Serializable
    {
        public int d;
        public int p;

        public Pair(int d, int p)
        {
            this.d = d;
            this.p = p;
        }
        
        
    }
    
}
