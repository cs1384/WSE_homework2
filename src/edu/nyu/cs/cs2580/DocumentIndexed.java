package edu.nyu.cs.cs2580;

import java.util.Vector;

/**
 * @CS2580: implement this class for HW2 to incorporate any additional
 * information needed for your favorite ranker.
 */
public class DocumentIndexed extends Document {
  private static final long serialVersionUID = 9184892508124423115L;
  private int docSize = -1;
  
  private String query = "";
  int occurance = 0;
  
  public DocumentIndexed(int docid) {
    super(docid);
  }
  
  public void setSearchTerm(String term){
    this.query = term;
  }
  
  public String getSearchedTerm(){
    return this.query;
  }
  
  public void setOccurance(int occur){
    this.occurance = occur;
  }
  
  public int getOccurance(){
    return this.occurance;
  }
  
  public void setSize(int size){
    this.docSize = size;
  }
  
  public int getSize(){
    return this.docSize;
  }
  
}
