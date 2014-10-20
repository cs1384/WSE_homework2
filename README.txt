Compile: 
$ javac -cp lib/jsoup-1.8.1.jar:lib/org.apache.commons.io.jar src/edu/nyu/cs/cs2580/*.java

Construct the index:
$ java -cp src:lib/jsoup-1.8.1.jar:lib/org.apache.commons.io.jar edu.nyu.cs.cs2580.SearchEngine --mode=index --options=conf/engine.conf

Start the server:
$ java -cp src -Xmx512m edu.nyu.cs.cs2580.SearchEngine --mode=serve --port=25816 --options=conf/engine.conf
