# Page-Rank
### Run PageRank
1. Get into project
2. Get into docker environment
### Open Docker
1. Get into directory of project

    ` cd src/main/java/ `
2. Put transition.txt into hdfs

   `hdfs dfs -rm -r /transition`
   
   `hdfs dfs -mkdir /transition`
   
   `hdfs dfs -put transitionsmall.txt /transition`
   
   `hdfs dfs -rm -r /output*`
   
   `hdfs dfs -rm -r /pagerank*`
   
   `hdfs dfs -mkdir /pagerank0`
   
   `hdfs dfs -put pr.txt /pagerank0`
   
   `hadoop com.sun.tools.javac.Main *.java `
   
   `jar cf pr.jar *.class`
   
   `hadoop jar pr.jar Driver /transition /pagerank /output 1`
   
   //args0: dir of transition.txt
   
   //args1: dir of PageRank.txt
   
   //args2: dir of unitMultiplication result
   
   //args3: times of convergence
   
3. Save the result in /pagerankN
4. Check the result

   `hdfs dfs -cat /pagerankN/*`
