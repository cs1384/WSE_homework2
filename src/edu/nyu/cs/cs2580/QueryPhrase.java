package edu.nyu.cs.cs2580;

import java.util.Scanner;
import java.util.Vector;

/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 * ["new york city"], the presence of the phrase "new york city" must be
 * recorded here and be used in indexing and ranking.
 */
public class QueryPhrase extends Query {

  public Vector<Vector<String>> _phrases = new Vector<Vector<String>>();
  
  public QueryPhrase(String query) {
    super(query);
    processQuery();
  }

  @Override
  public void processQuery() {
    if(_query==null)
      return;
    char[] chars = _query.trim().toCharArray();
    System.out.println(_query);
    StringBuilder sb = new StringBuilder();
    Vector<String> phrase = new Vector<String>();
    boolean inPhrase = false;
    for(char c : chars){
      switch(c){
      case '"':
        if(inPhrase && sb.length()>0){
          phrase.add(sb.toString());
          sb.setLength(0);
          sb.trimToSize();
          _phrases.add(phrase);
          phrase.clear();
        }
        inPhrase = inPhrase?false:true;
        break;
      case ' ':
        if(inPhrase && sb.length()>0){
          phrase.add(sb.toString());
          sb.setLength(0);
          sb.trimToSize();
        }else if(!inPhrase && sb.length()>0){
          _tokens.add(sb.toString());
          sb.setLength(0);
          sb.trimToSize();
        }
        break;
      default:
        sb.append(c);
        break;
      }
    }
    if(sb.length()>0){
      _tokens.add(sb.toString());
      sb.setLength(0);
      sb.trimToSize();
    }
  }
  
  public static void main(String args[]){
    QueryPhrase qp = new QueryPhrase(" test \"tin is cool\" \"kiss\" dad");
    System.out.println(qp._tokens);
    System.out.println(qp._phrases.size());
  }
}
