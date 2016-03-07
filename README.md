# Hortonworks-Sandbox-Hadoop-MapReduce

### Introduction

This project consists of two MapReduce jobs running in Hortonworks Sandbox, which is a self-contained virtual machine with Apache Hadoop pre-configured. 

#### Counting Attributes: 

Count the number of impressions, clicks, conversions for each of the campaigns in the data set. The input file is in CSV format. The first few fields in file are:
- ymdh
- user_id
- impression
- click
- conversion
- campaign_id
- creative_id
- ...

Each line of the output file obeys the following format: <br>
	__campaign_id, count of impressions, count of clicks, count of conversions__

###### Usage:
	Environment setup:
	Download HDP 2.4 on Hortonworks Sandbox, load it to your virtual machine (like virtualBox)
	After login virtual machine via ssh; open http://127.0.0.1/8080 and login as "maria_dev/maria_dev" (usrname/password); click "HDFS files" on navigation bar; create folders /usr/root/mapredJobs/input; upload data into /usr/root/mapredJobs/input

	Compile & jar (in virtual machine): 
	javac -cp /usr/hdp/2.4.0.0-169/hadoop/*:/usr/hdp/2.4.0.0-169/hadoop-mapreduce/* countingArray.java
	jar -cvf countingArray.jar *.class
	
	Run: 
	hadoop jar countingArray.jar countingArray mapredJobs/input/data mapredJobs/output

#### Self Theta Join 

Find the click events from different users that are close to each other. The required logic can be represented as the following SQL:

	SELECT a.ymdh, a.user_id, b.user_id
	FROM   data a, data b
	WHERE  a.click = 1 AND b.click = 1
	AND    a.user_id != null AND b.user_id != null
	AND    a.user_id < b.user_id
	AND    abs(TIMESTAMPDIFF(SECOND, a.ymdh, b.ymdh)) < 2;

###### Usage: 

	Compile & jar (in virtual machine): 
	javac -cp /usr/hdp/2.4.0.0-169/hadoop/*:/usr/hdp/2.4.0.0-169/hadoop-mapreduce/* selfThetaJoin.java
	jar -cvf selfThetaJoin.jar *.class
	
	Run: 
	hadoop jar selfThetaJoin.jar selfThetaJoin mapredJobs/input/data mapredJobs/output
