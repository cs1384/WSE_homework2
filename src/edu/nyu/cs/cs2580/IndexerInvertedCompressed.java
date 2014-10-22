package edu.nyu.cs.cs2580;

import java.io.IOException;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import org.apache.commons.io.IOUtils;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends Indexer implements Serializable
{
    
    private Map<String, Integer> posMap = new HashMap<String, Integer>();
    private Vector<String> wordList = new Vector<String>();
    private Vector<Integer> byteArraySizes = new Vector<Integer>();
    
    //private Vector<ByteArray2> vectoredMap = new Vector<ByteArray2>();
    //private Vector<byte[]> vectoredMap = new Vector<byte[]>();
    //private Vector<Vector<Byte>> vectoredMap = new Vector<Vector<Byte>>();
    
    //private Map<String, byte[]> _index = new HashMap<String, byte[]>();
    private Vector<byte[]> _index = new Vector<byte[]>();
    
    //private Map<String, ArrayList<Pair> > skipList = new HashMap<String, ArrayList<Pair>>();
    private Vector<ArrayList<Pair> > skipListTemp = new Vector<ArrayList<Pair>>();
    private Vector<Pair[] > skipList = new Vector<Pair[]>();
    
    private Map<String, Integer> _termCorpusFrequency = new HashMap<String, Integer>();
    private Map<String, Integer> _corpusDocFrequencyByTerm = new HashMap<String, Integer>();
    //map url to docid to support documentTermFrequency method
    private Map<String, Integer> _urlToDoc = new HashMap<String, Integer>();
    //to store and quick access to basic document information such as title 
    private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
    
    private int mergedID = 0;

    
    
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

    private void constructPartialIndex(int id, List<File> listOfFiles)
    {
        Map<String, Vector<Posting>> _indexTemp = new HashMap<String, Vector<Posting>>();
        try
        {
            int count = 0;
            
            for (File file : listOfFiles)
            {
                String text = TestParse2.getPlainText(file);
                String title = file.getName().replace('_', ' ');
                text = title + " " + text;
                
                processDocument(text, title, _indexTemp); //process each webpage
                
                count++;
                
                if(count % 100 == 0)
                    System.out.println("Processed " + count + " documents");
            }
        } 
        catch (Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            //write the index out to a file, in alphabetical order:
            Set<String> keys = _indexTemp.keySet();
            Vector<String> keysVec = new Vector<String>();
            for(String s : keys)
                keysVec.add(s);
            Collections.sort(keysVec);
                
            System.out.println("Writing file...");
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/partial_cmpr_corpus_" + id + ".txt")));
            StringBuilder sb = new StringBuilder();
            
            //sb.append("\n");
            for(String term : keysVec)
            {
                Vector<Posting> pv = _indexTemp.get(term);
                
                sb.append(term).append(" ");
                
                for(Posting p : pv)
                {
                        sb.append(p.did).append(" ");
                        sb.append(p.offsets.size()).append(" ");
                        
                        for(Integer o : p.offsets)
                        {
                            sb.append(o).append(" ");
                        }
                }
                sb.append("\n");
                
            }
            bw.write(sb.toString());
            bw.close();
            System.out.println("Partially Indexed " + Integer.toString(_numDocs) + " docs with " + Long.toString(_totalTermFrequency) + " terms.");
            
        }
        catch(Exception e)
        {
            e.printStackTrace();;
        }
    }
    
    @Override
    public void constructIndex() throws IOException
    {
        int count = 0;
        
        try
        {
            String corpusFolder = _options._corpusPrefix + "/";
            System.out.println("Construct index from: " + corpusFolder);
            
            File folder = new File(corpusFolder);
            ArrayList<File> fileList = new ArrayList<File>();
            for (final File file : folder.listFiles())
            {
                fileList.add(file);
            }
            
            int lower=0, upper = 500;
            int id = 0;
            for(id=0;lower < fileList.size();id++)
            {
                if(upper > fileList.size())
                    upper = fileList.size();

                constructPartialIndex(id, fileList.subList(lower, upper));
                lower = upper;
                upper += 500;
                count++;
            }
        } 
        catch (Exception e)
        {
            e.printStackTrace();
        }

        // ************************************************
        //   Now merge the partial indices lying on disk
        //   The last merge also compressed
        // ***********************************************
        System.out.println("Merging files...");
        try
        {
            File f = new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_0.txt");
            f.createNewFile();            
            int i;
            for(i=0;i<count-1;i++)
            {
                MergeWriter mw = new MergeToText(i);
                mergeIndices(i, mw) ;
                System.out.println("Merged " + (i+1) + " / " + count);
            }
            System.out.println("Compressing file merge...");
            MergeWriter mw = new MergeToBytes(count);
            mergeIndices(i, mw) ;
            System.out.println("Merged " + (i+1) + " / " + count);
        }
        catch(Exception e)
        {
            e.printStackTrace();;
        } 
        
        
        // ************************************************
        //   Now read the file and put in the data structure
        // ***********************************************
        
        
        mergedID = count;
        
        String indexFile = _options._indexPrefix + "/compressed_corpus.idx";
        System.out.println("Store index to: " + indexFile);
        ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
        writer.writeObject(this); //write the entire class into the file
        writer.close();
        
    }


    public void processDocument(String content, String title, Map<String, Vector<Posting>> _indexTemp)
    {

        DocumentIndexed doc = new DocumentIndexed(_documents.size() + 1);
        doc.setTitle(title);
        String text = content;
        
        int docWords = ProcessTerms(text, doc._docid, _indexTemp);
        doc.setSize(docWords);
        
        //assign random number to doc numViews
        int numViews = (int) (Math.random() * 10000);
        doc.setNumViews(numViews);

        String url = "en.wikipedia.org/wiki/" + title;
        doc.setUrl(url);
        _urlToDoc.put(url, doc._docid); //build up urlToDoc map

        _documents.add(doc);
        _numDocs++;
        
    }

    public int ProcessTerms(String content, int docid, Map<String, Vector<Posting>> _indexTemp)
    {
        Stemmer stemmer = new Stemmer();
        
        //map for the process of this doc
        Map<String, Vector<Integer>> op = new HashMap<String, Vector<Integer>>();
        int offset = 1; //offset starts from 1
        Scanner s = new Scanner(content);
        int docWords = 0;
        while (s.hasNext())
        {
            //put offsets into op map
            String token = s.next();
            
            
            stemmer.add(token.toCharArray(), token.length());
            stemmer.stem();
            token = stemmer.toString();
            
            token = token.toLowerCase();
            docWords++;
            
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
            } 
            else
            {
                _termCorpusFrequency.put(token, 1);
            }
            
            _totalTermFrequency++;
            docWords++;
            offset++;
        }
        s.close();
        
        //store doc map info into index map 
        for (String term : op.keySet())
        {
            Posting posting = new Posting(docid);
            posting.offsets = op.get(term);
            if (_indexTemp.containsKey(term))
            {
                _indexTemp.get(term).add(posting);
            } else
            {
                Vector<Posting> docTracker = new Vector<Posting>();
                docTracker.add(posting);
                _indexTemp.put(term, docTracker);
            }
        }
        return docWords;
    }

    /**
     * This function merges two files reading only two lines at a time.
     * It's behavior is similar to the MERGE from MERGE-SORT, which allows to 
     * merge files without reading everything at once.
     * 
     * It writes the info to the files using classes implementing MergeWriter
     * The purpose being that the last run of this function prints compressed info.
     * Having an interface allows the algorithm to remain oblivious to the write process 
     * and therefore clean.
     * @param id
     * @param mw 
     */
    public void mergeIndices(int id, MergeWriter mw) 
    {
        try
        {
            BufferedReader br1 = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/partial_cmpr_corpus_" + id + ".txt")));
            BufferedReader br2 = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_" + id + ".txt")));
            
            //now walk the files, and write to a new file
            int i=1, j=1;
            String file1Line = br1.readLine();
            String file2Line = br2.readLine();
                
            while(file1Line != null && file2Line != null)
            {
                Scanner file1LScan = new Scanner(file1Line);
                Scanner file2LScan = new Scanner(file2Line);
        
                String word1 = file1LScan.next();
                String word2 = file2LScan.next();
                
                if(word1.compareTo(word2) < 0)
                {
                    mw.writeLine(file1Line);
                    file1Line = br1.readLine();
                    i++;
                }
                else if(word1.compareTo(word2) > 0)
                {
                    mw.writeLine(file2Line);
                    file2Line = br2.readLine();
                    j++;
                }
                else
                {
                    //need to merge
                    //parse tokens1 and tokens2 into a postings list
                    Vector<Posting> allPosting = new Vector<Posting>();
                    
                    while(file1LScan.hasNext())
                    {
                        int docId = Integer.parseInt(file1LScan.next());   
                        int numOffsets = Integer.parseInt(file1LScan.next());  
                        
                        Posting p = new Posting(docId);
                        for(int l=0;l<numOffsets;l++)
                            p.offsets.add(Integer.parseInt(file1LScan.next()));
                        allPosting.add(p);
                    }
                    
                    
                    while(file2LScan.hasNext())
                    {
                        int docId = Integer.parseInt(file2LScan.next());   
                        int numOffsets = Integer.parseInt(file2LScan.next());  
                        
                        Posting p = new Posting(docId);
                        for(int l=0;l<numOffsets;l++)
                            p.offsets.add(Integer.parseInt(file2LScan.next()));    //***
                        allPosting.add(p);
                    }

                    
                    Collections.sort(allPosting, Comparator);
                    
                    file1Line = br1.readLine();
                    file2Line = br2.readLine();
                     
                    StringBuilder sb = new StringBuilder();
                    sb.append(word1).append(" ");     
                        
                    for(Posting p : allPosting)
                    {
                            sb.append(p.did).append(" ");
                            sb.append(p.offsets.size()).append(" ");
                        
                            for(Integer o : p.offsets)
                            {
                                sb.append(o).append(" ");
                            }
                    }

                    mw.writeLine(sb.toString());
                    i++;
                    j++;
                }
            }
            
            while(file1Line != null)
            {
                mw.writeLine(file1Line);
                file1Line = br1.readLine();
            }
            while(file2Line != null)
            {
                mw.writeLine(file2Line);
                file2Line = br2.readLine();
            }
            mw.close();
            br1.close();
            br2.close();
            
            //delete the two files that were consumed here
            File f1 = new File(_options._indexPrefix + "/partial_cmpr_corpus_" + id + ".txt");
            File f2 = new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_" + id + ".txt");
            
            f1.delete();
            f2.delete();
            
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    
        
    //postingList contains the string name too, discard it
    void savePostingListAsBytes(String postingList, FileOutputStream outputPosting, BufferedWriter outputSkipList)
    {
        Scanner lineScan = new Scanner(postingList.trim());
        
        
        String term = null;
        if(!lineScan.hasNext()) 
            return;
                    
        term = lineScan.next();
        
        if(!lineScan.hasNext()) 
            return;

        Vector<Byte> byteVec = new Vector<Byte>();

        posMap.put(term, wordList.size());
        wordList.add(term);

        ArrayList<Pair> pl = new ArrayList<Pair>();

        while(lineScan.hasNext())
        {
            int docid = Integer.parseInt(lineScan.next());
            int numOffsets = Integer.parseInt(lineScan.next());

            if(pl.size() > 0)
            {

                Pair p = pl.get(pl.size()-1);
                if(byteVec.size() - p.p > 10000)
                {
                    pl.add(new Pair(docid, byteVec.size()));
                }
            }
            else
            {
                pl.add(new Pair(docid, byteVec.size()));
            }


            byte bArray[] = new byte[4];

            int sizeReq = VByteEncoder.encode(docid, bArray);
            for(int bi=0;bi<sizeReq;bi++)
                byteVec.add(bArray[bi]);  

            sizeReq = VByteEncoder.encode(numOffsets, bArray);
            for(int bi=0;bi<sizeReq;bi++)
                byteVec.add(bArray[bi]);  


            int firstOffset = Integer.parseInt(lineScan.next());
            int prev = firstOffset;

            sizeReq = VByteEncoder.encode(prev, bArray);
            for(int bi=0;bi<sizeReq;bi++)
                byteVec.add(bArray[bi]);  

            for(int l=1;l<numOffsets;l++)
            {
                int offset = Integer.parseInt(lineScan.next());

                int x = offset;
                sizeReq = VByteEncoder.encode(x - prev, bArray);
                for(int bi=0;bi<sizeReq;bi++)
                    byteVec.add(bArray[bi]);  


                prev = x;
            }
            
        }

        byte array[] = new byte[byteVec.size()];
        for(int p=0;p<byteVec.size();p++)
        {
            array[p] = byteVec.get(p);
        }
        
        //Now write this to the byte file
        try
        {
            IOUtils.write(array, outputPosting);
            byteArraySizes.add(array.length);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
        //Now write the skip lists to a file, as integers
        try
        {
            //BufferedWriter bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/skippointers.sp"), true));
            StringBuilder sb = new StringBuilder();
            for(int i=0;i<pl.size();i++)
            {
                sb.append(pl.get(i).d);
                sb.append(" ");
                sb.append(pl.get(i).p);
                sb.append(" ");
            }
            sb.append("\n");
            outputSkipList.write(sb.toString());
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    
    
    
    
    @Override
    public void loadIndex() throws IOException, ClassNotFoundException
    {
        String indexFile = _options._indexPrefix + "/compressed_corpus.idx";
        System.out.println("Load index from: " + indexFile);

        ObjectInputStream reader = null;
        IndexerInvertedCompressed loaded = null;
        try
        {
            reader = new ObjectInputStream(new FileInputStream(indexFile));
            loaded = (IndexerInvertedCompressed) reader.readObject();
        }
        catch(FileNotFoundException fne)
        {
            System.out.println("Please use the serialized object 'compressed_corpus.idx' for the compressed index");
            return;
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return;
        }

        if(loaded == null)
        {
            System.out.println("Error loading index.");
            return ;
        }
        
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
        this.wordList = loaded.wordList;
        this._termCorpusFrequency = loaded._termCorpusFrequency;
        this._urlToDoc = loaded._urlToDoc;
        this.mergedID = loaded.mergedID;
        this.byteArraySizes = loaded.byteArraySizes;
        
        reader.close();
        
        System.out.println("Reading index files...");
        
        byte newArray[];
        FileInputStream input = new FileInputStream(new File(_options._indexPrefix + "/posting.pl"));
        
        for(int i=0;i<byteArraySizes.size();i++)
        {
            int lenToRead = byteArraySizes.get(i);
            newArray = IOUtils.toByteArray(input, lenToRead);
            
            _index.add(newArray);
        }
        
        //now read skiplist
        
        BufferedReader inputSkiplist = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/skiplist.sp")));
        
        String line;
        while((line = inputSkiplist.readLine()) != null)
        {
            String tokens[] = line.split(" ");
            Pair pairList[] = new Pair[tokens.length/2];
            for(int i=0;i<tokens.length;i+=2)
            {
                Pair p = new Pair(Integer.parseInt(tokens[i]), Integer.parseInt(tokens[i+1]));
                pairList[i/2] = p;
            }
            
            skipList.add(pairList);
        }
        
        System.out.println(Integer.toString(_numDocs) + " documents loaded " + "with " + Long.toString(_totalTermFrequency) + " terms!");
        
        //printIndex();
        /*
        //int x = next_pos("abc", 1, 0);
        //System.out.println("x = " + x);
        Vector<String> phrase = new Vector<String>();
        phrase.add("abc");
        phrase.add("def");
        Vector<DocumentIndexed> vec = allDocPhrase(phrase);
        
        System.out.println("Results");
        for(DocumentIndexed d: vec)
        {
            System.out.println("d = " + d._docid);
        }
        */
    }
    

    private int next(String w, int docId)
    {
        if(!posMap.containsKey(w))
            return Integer.MAX_VALUE;
        
        int wordIndex = posMap.get(w);

        //Scan skip list to find this doc
        Pair pl[] = skipList.get(wordIndex);
        if(pl == null)
            System.out.println("DSDS");
        
        //System.out.println("pl.size() = " + pl.length);
        int prev = 0;
        int offset = 0;
        for(Pair p : pl)
        {
            prev = offset;
            offset = p.p;
            
            if(p.d >= docId)
                break;

        }
        
        
        offset = prev;
        byte bList[] = _index.get(wordIndex);

        int i = 0;
        Integer nextLoc = offset;
        boolean found = false;
        while(i < bList.length)
        {
            int x[] = VByteEncoder.getFirstNum(bList, nextLoc);
            int doc = x[0];
            nextLoc = x[1];
            
            if(doc > docId)
                found = true;

            x = VByteEncoder.getFirstNum(bList, nextLoc);
            int numOccur = x[0];
            nextLoc = x[1];
            
            for(int j=0;j<numOccur;j++)
            {
                x = VByteEncoder.getFirstNum(bList, nextLoc);
                nextLoc = x[1];
            }
            
            //next doc's offset:
            i = nextLoc; //1 because of numOffsets
            
            if(found)
                return doc;
        }
        
        //now do a linear search to find the doc after docId
        return Integer.MAX_VALUE;
    }
    
    /**
    This function uses the functionality from next(), which looks for docId
    * We will adapt this so that we start with docId-1, then look for docId and it not found, we return
    */
    private int next_pos(String w, int docId, int pos)
    {
        docId -= 1; //need to do this so we can use legacy code
        if(!posMap.containsKey(w))
            return Integer.MAX_VALUE;
        
        int wordIndex = posMap.get(w);
        
        //System.out.println("word: " + w + ",  at index:" + wordIndex);
        //System.out.println("next: " + w + ", " + docId);
        //Scan skip list to find this doc
        Pair pl[] = skipList.get(wordIndex);
        if(pl == null)
        {
            System.out.println("Unexpected Error!");
            return Integer.MAX_VALUE;
        }
        
        //System.out.println("pl.size() = " + pl.length);
        int prev = 0;
        int offset = 0;
        for(Pair p : pl)
        {
            prev = offset;
            offset = p.p;
            
            if(p.d >= docId)
                break;

        }
        
        
        offset = prev;
        byte bList[] = _index.get(wordIndex);

        int i = 0;
        Integer nextLoc = offset;
        boolean found = false;
        while(i < bList.length)
        {
            int x[] = VByteEncoder.getFirstNum(bList, nextLoc);
            int doc = x[0];
            nextLoc = x[1];
            
            if(doc == docId+1)
            {
                
                found = true;
            }

            x = VByteEncoder.getFirstNum(bList, nextLoc);
            int numOccur = x[0];
            nextLoc = x[1];
            
            for(int j=0;j<numOccur;j++)
            {
                x = VByteEncoder.getFirstNum(bList, nextLoc);
                int offsetX = x[0];
                
                if(found && offsetX > pos)
                    return offsetX;
                
                nextLoc = x[1];
            }
            
            //next doc's offset:
            i = nextLoc; //1 because of numOffsets

        }
        
        //now do a linear search to find the doc after docId
        return Integer.MAX_VALUE;
    }
    
    

    //This tells us which byte number the do starts from, for faster lookup
    private int next_pos(String w, int docId, int pos, int byteOffset)
    {
        docId -= 1; //need to do this so we can use legacy code
        if(!posMap.containsKey(w))
            return Integer.MAX_VALUE;
        
        int wordIndex = posMap.get(w);
        
        //System.out.println("word: " + w + ",  at index:" + wordIndex);
        //System.out.println("next: " + w + ", " + docId);
        //Scan skip list to find this doc
        
        Pair pl[] = skipList.get(wordIndex);
        if(pl == null)
        {
            System.out.println("Unexpected Error!");
            return Integer.MAX_VALUE;
        }
        
        //System.out.println("pl.size() = " + pl.length);
        int prev = 0;
        int offset = 0;
        for(Pair p : pl)
        {
            prev = offset;
            offset = p.p;
            
            if(p.d >= docId)
                break;

        }
        
        
        offset = prev;
        
        byte bList[] = _index.get(wordIndex);

        int i = 0;
        //Integer nextLoc = byteOffset;
        Integer nextLoc = offset;
        boolean found = false;
        while(i < bList.length)
        {
            //System.out.println("i = " + i);
            int x[] = VByteEncoder.getFirstNum(bList, nextLoc);
            int doc = x[0];
            nextLoc = x[1];
            
            if(doc == docId+1)
            {
                
                found = true;
            }
            //System.out.println("doc = " + doc);
            x = VByteEncoder.getFirstNum(bList, nextLoc);
            int numOccur = x[0];
            //System.out.println("numOccur = " + numOccur);
            
            nextLoc = x[1];
            
            //System.out.println("nextLoc = " + nextLoc);
            
            int total = 0;
            for(int j=0;j<numOccur;j++)
            {
                x = VByteEncoder.getFirstNum(bList, nextLoc);
                int offsetX = x[0];
                //System.out.println("offsetX = " + offsetX);
                total += offsetX;
                
                if(found && total > pos)
                    return total;
                
                nextLoc = x[1];
            }
            
            //next doc's offset:
            i = nextLoc; //1 because of numOffsets

        }
        
        //now do a linear search to find the doc after docId
        return Integer.MAX_VALUE;
    }
    
    
    private int findDoc(String w, int docId)
    {
        if(!posMap.containsKey(w))
            return Integer.MAX_VALUE;
        
        int wordIndex = posMap.get(w);

        //Scan skip list to find this doc
        Pair pl[] = skipList.get(wordIndex);
        if(pl == null)
            System.out.println("DSDS");
        
        //System.out.println("pl.size() = " + pl.length);
        int prev = 0;
        int offset = 0;
        for(Pair p : pl)
        {
            prev = offset;
            offset = p.p;
            
            if(p.d >= docId)
                break;

        }
        
        
        offset = prev;
        byte bList[] = _index.get(wordIndex);

        int i = 0;
        Integer nextLoc = offset;
        boolean found = false;
        while(i < bList.length)
        {
            int x[] = VByteEncoder.getFirstNum(bList, nextLoc);
            int doc = x[0];
            
            if(doc >= docId)
                return nextLoc;
            
            nextLoc = x[1];
            
            if(doc > docId)
                found = true;

            x = VByteEncoder.getFirstNum(bList, nextLoc);
            int numOccur = x[0];
            nextLoc = x[1];
            
            for(int j=0;j<numOccur;j++)
            {
                x = VByteEncoder.getFirstNum(bList, nextLoc);
                nextLoc = x[1];
            }
            
            //next doc's offset:
            i = nextLoc; //1 because of numOffsets

        }
        
        //now do a linear search to find the doc after docId
        return Integer.MAX_VALUE;
    }
    
    
    
    @Override
    public Document getDoc(int docid)
    {
        docid = docid - 1;
        
        return (docid >= _documents.size() || docid < 0) ? null : _documents.get(docid);
    }

    @Override
    public DocumentIndexed nextDoc(Query query, int docid)
    {
        Vector<String> queryVec = query._tokens;

        ArrayList<Integer> pos = new ArrayList<Integer>();
        for(int i=0;i<queryVec.size();i++)
        {
            int n = next(queryVec.get(i), docid);
            //System.out.println("got n = " + n + "  ,  for docid = " + docid);
            if(n == Integer.MAX_VALUE)
            {
                return null;
            }
            
            pos.add(n);
        }
        
        boolean mismatch = false;
        for(int i=1;i<queryVec.size();i++)
        {
            //System.out.println("pos.get(i-1) = " + pos.get(i-1));
            //System.out.println("pos.get(i) = " + pos.get(i));
                
            if(pos.get(i-1).intValue() != pos.get(i).intValue())
            {
                //System.out.println("mismatch");
                mismatch = true;
                break;
            }
        }
        
        if(mismatch)
        {
            //System.out.println("mismatch");
            int max = 0;
            for(int i=0;i<queryVec.size();i++)
            {
                if(pos.get(i) > max)
                    max = pos.get(i);
            }
            //System.out.println("max = " + max);
            return nextDoc(query, max-1);
        }
        
        /*
        System.out.println("pos.get(0) = " + (pos.get(0)));
        System.out.println("pos.get(0)-1 = " + (pos.get(0)-1));
        System.out.println("returning doc = " +  _documents.get(pos.get(0)-1)._docid );
        
        System.out.println("");
        */
        
        return _documents.get(pos.get(0)-1);
        //return new DocumentIndexed(pos.get(0));
    }
    
    
    public Vector<DocumentIndexed> allDocPhrase(Vector<String> phrase)
    {
        Vector<DocumentIndexed> results = new Vector<DocumentIndexed>();
        
        //first, get posting list sizes for each term, and look at docs in the smalles list
        int min = Integer.MAX_VALUE;
        String minTerm = "";
        int minIndex = 0;
        for(int i=0;i<phrase.size();i++)
        {
            //System.out.println("word = " + phrase.get(i));
            String w = phrase.get(i);
            //System.out.println("w = " + w);
            if(!posMap.containsKey(w))
                return results;
        
            int wordIndex = posMap.get(w);
            //System.out.println("wordIndex = " + wordIndex);
            //System.out.println("length = " + _index.get(wordIndex).length);
            
            if(min >= _index.get(wordIndex).length)
            {
                min = _index.get(wordIndex).length;
                minTerm = w;
                minIndex = wordIndex;
                //System.out.println("minIndex = " + minIndex);
            }
        }
        
        //System.out.println("final minIndex = " + minIndex);
        //now get docs from posting list for minTerm
        byte bArray[] = _index.get(minIndex);
        ArrayList<Integer> docsForMinTerm = getAllDocsInPosting(bArray);
        
        for(Integer doc : docsForMinTerm)
        {
            //System.out.println("x doc = " + doc);
        
        }
        for(Integer doc : docsForMinTerm)
        {
            //System.out.println("doc = " + doc);
            int docByte = findDoc(minTerm, doc);
            int x = nextDocPhrase(phrase, doc, 0, docByte);
            if(x != Integer.MAX_VALUE)
            {
                results.add(_documents.get(doc-1));
                //System.out.println("return " + (doc-1));
            }
        }
        return results;
    }
    /*
        
    */
    public int nextDocPhrase(Vector<String> phrase, int docid, int posBefore, int docByte)
    {
        ArrayList<Integer> pos = new ArrayList<Integer>();
        
        for(int i=0;i<phrase.size();i++)
        {
            int n = next_pos(phrase.get(i), docid, posBefore, docByte);
            
            //System.out.println("phrase = " + phrase.get(i) + "    got n = " + n + "  ,  for docid = " + docid);
            if(n == Integer.MAX_VALUE)
            {
                return n;
            }
            
            pos.add(n);
        }
        
        boolean mismatch = false;
        for(int i=1;i<pos.size();i++)
        {
            //System.out.println("pos.get(i-1) = " + pos.get(i-1));
            //System.out.println("pos.get(i) = " + pos.get(i));
                
            if(pos.get(i-1).intValue() != pos.get(i).intValue()-1)
            {
                //System.out.println("mismatch");
                mismatch = true;
                break;
            }
        }
        
        if(mismatch)
        {
            //System.out.println("mismatch");
            int max = 0;
            int min = Integer.MAX_VALUE;
            for(int i=0;i<phrase.size();i++)
            {
                if(pos.get(i) > max)
                    max = pos.get(i);
                if(pos.get(i) < min)
                    min = pos.get(i);
            }
            //System.out.println("min = " + min + " for doc id = " + docid);
            //return nextDocPhrase(phrase, docid, max-1);
            return nextDocPhrase(phrase, docid, min, docByte);
        }
        
        /*
        System.out.println("pos.get(0) = " + (pos.get(0)));
        System.out.println("pos.get(0)-1 = " + (pos.get(0)-1));
        System.out.println("returning doc = " +  _documents.get(pos.get(0)-1)._docid );
        
        System.out.println("");
        */
        
        return pos.get(0);
        //return new DocumentIndexed(pos.get(0));
    }
    
    ArrayList<Integer> getAllDocsInPosting(byte bList[])
    {
        ArrayList<Integer> list = new ArrayList<Integer>();
        int i = 0;
        Integer nextLoc = 0;
        //System.out.println("bList.length = " + bList.length);
        while(i < bList.length)
        {
            int x[] = VByteEncoder.getFirstNum(bList, nextLoc);
            int doc = x[0];
            //System.out.println("add " + doc);
            list.add(doc);
            nextLoc = x[1];
            
            x = VByteEncoder.getFirstNum(bList, nextLoc);
            int numOccur = x[0];
            //System.out.println("num occur = " + numOccur);
            nextLoc = x[1];
            
            for(int j=0;j<numOccur;j++)
            {
                x = VByteEncoder.getFirstNum(bList, nextLoc);
                nextLoc = x[1];
            }
            
            //next doc's offset:
            i = nextLoc; //1 because of numOffsets
            
        }
        return list;
    }
    
    
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
        //System.out.println("term = " + term + " , url = " + url);
        if (_urlToDoc.containsKey(url))
        {
            //System.out.println("contains");
            int did = _urlToDoc.get(url);
            
            String key = term;
            int i = posMap.get(key);
            byte vec[] = _index.get(i);

            //System.out.println("key = " + key);
            //System.out.print(key + ": ");
            
            ArrayList<Integer> nums = VByteEncoder.decode(vec);
            for (int j = 0; j < nums.size();)
            {
                int doc = nums.get(j);
                int numOffsets = nums.get(j+1);
            
                if(doc == did)
                    return numOffsets;
                
                j=j+2+numOffsets;
            }
            
        } 
        else
        {
            System.out.println("not contains");
            return 0;
        }
        return 0;
    }

    /**
     * For testing only... converts the byte posting list to integers 
     * and prints against each term
     */
    private void printIndex()
    {
        
        for(int i=0;i<_index.size();i++)
        {
            String key = wordList.get(i);
            byte vec[] = _index.get(i);

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
    
    
    

    class MergeToText implements MergeWriter
    {
        BufferedWriter outBw;
        public MergeToText(int id)
        {
            try
            {
                outBw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/partial_cmpr_corpus_merged_" + (id+1) + ".txt")));
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        
        public void writeLine(String s)
        {
            try
            {
                outBw.write(s);
                outBw.write("\n");
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        
        public void close()
        {
            try
            {
                outBw.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    
    class MergeToBytes implements MergeWriter
    {
        FileOutputStream outputPosting = null;
        BufferedWriter outputSkiplist = null;
        public MergeToBytes(int id)
        {
            try
            {
                outputPosting = new FileOutputStream(new File(_options._indexPrefix + "/posting.pl"));
                outputSkiplist = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/skiplist.sp")));
            }
            catch(Exception e)
            {
                    e.printStackTrace();
            }
        }
        
        public void writeLine(String s)
        {
            savePostingListAsBytes(s, outputPosting, outputSkiplist);
        }
        
        public void close()
        {
            try
            {
                outputPosting.close();
                outputSkiplist.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
          
    }
}

