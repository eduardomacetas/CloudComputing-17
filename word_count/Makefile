HADOOP_HOME = /home/rustycode/storage/Apps/hadoop-2.8.1

run: jar
#hadoop fs -rm -f -r  /user/cloudera/wordcount/output
	${HADOOP_HOME}/bin/hadoop jar wordcount.jar ../../../word_counter/data/BOM.txt output

run_caseSensitive: jar
	hadoop fs -rm -f -r  /user/cloudera/wordcount/output
	hadoop jar wordcount.jar com.rustycode.WordCount -Dwordcount.case.sensitive=true /user/cloudera/wordcount/input /user/cloudera/wordcount/output 

run_stopwords: jar stopwords
	hadoop fs -rm -f -r  /user/cloudera/wordcount/output
	hadoop jar wordcount.jar com.rustycode.WordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output -skip /user/cloudera/wordcount/stop_words.text

compile: out/com/rustycode/WordCount.class

jar: wordcount.jar

update_path:
	export PATH=$PATH:~/storage/Apps/android-studio/jre/bin:/home/rustycode/storage/Apps/hadoop-2.8.1/bin

change_dir:
	cd ~/storage/Dev/cloud_computing/word_count/out/production

wordcount.jar: change_dir #out/com/rustycode/WordCount.class
	jar -cvfe wordcount.jar com.rustycode.word_count.WordCount -C word_count/ .
#jar -cvf wordcount.jar -C out/production/word_count/com/rustycode/word_count/ .

out/com/rustycode/WordCount.class: WordCount.java
	mkdir -p out
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* WordCount.java -d out -Xlint

clean:
	rm -rf out wordcount.jar

data:
	hadoop fs -rm -f -r /user/cloudera/wordcount/input
	hadoop fs -mkdir /user/cloudera/wordcount
	hadoop fs -mkdir /user/cloudera/wordcount/input
	echo "Hadoop is an elephant" > file0
	echo "Hadoop is as yellow as can be" > file1
	echo "Oh what a yellow fellow is Hadoop" > file2
	hadoop fs -put file* /user/cloudera/wordcount/input
	rm file*

poetry:
	hadoop fs -rm -f -r /user/cloudera/wordcount/input
	hadoop fs -mkdir /user/cloudera/wordcount/input
	echo -e "Hadoop is the Elephant King! \\nA yellow and elegant thing.\\nHe never forgets\\nUseful data, or lets\\nAn extraneous element cling! "> HadoopPoem0.txt
	echo -e "A wonderful king is Hadoop.\\nThe elephant plays well with Sqoop.\\nBut what helps him to thrive\\nAre Impala, and Hive,\\nAnd HDFS in the group." > HadoopPoem1.txt
	echo -e "Hadoop is an elegant fellow.\\nAn elephant gentle and mellow.\\nHe never gets mad,\\nOr does anything bad,\\nBecause, at his core, he is yellow." > HadoopPoem2.txt
	hadoop fs -put HadoopP* /user/cloudera/wordcount/input
	rm HadoopPoem*

showResult:
	hadoop fs -cat /user/cloudera/wordcount/output/*
	
stopwords:
	hadoop fs -rm -f /user/cloudera/wordcount/stop_words.text
	echo -e "a\\nan\\nand\\nbut\\nis\\nor\\nthe\\nto\\n.\\n," >stop_words.text
	hadoop fs -put stop_words.text /user/cloudera/wordcount/

