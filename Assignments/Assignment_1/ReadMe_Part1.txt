Readme File - Assignment 1 - Big Data

Step 1. Create a folder assignment_1 inside /user/axp161730
 	Eg : hdfs dfs -mkdir /user/axp161730/assignment_1

Step 2. Create subfolders part1 & part2 inside assignment_1
	Eg : hdfs dfs -mkdir /user/axp161730/assignment_1/part1
	     hdfs dfs -mkdir /user/axp161730/assignment_1/part2

Step 3. Run the program Assi_1_Part_1.java
	Eg : hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.Assi_1_Part_1

Step 4. Run the program TwitterApp.java
	Eg : hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.TwitterApp

Step 5. View the files in both part1 and part2 folders.
	Eg : hdfs dfs -ls /user/axp161730/assignment_1/part1
	     hdfs dfs -ls /user/axp161730/assignment_1/part2

Topic searched to download tweets and time lines used: 

Topic : Trump
Time lines : 
	"2016-01-01",
	"2016-02-01",
	"2016-03-01",
	"2016-04-01",
	"2016-05-01",
	"2016-06-01"

Maximum query count given is 100.
Sizes of each : 12kb-16Kb


Remove 
hdfs dfs -rm -r -f /user/axp161730/wordcount