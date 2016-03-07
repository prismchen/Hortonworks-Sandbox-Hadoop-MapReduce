Method: 
	Map: for each line of data, we shift the timestamp by -1s, 0s and 1s, and we output three key-value pairs in the format:
	<timestamp, [user_id, time shift (sec))]>
	
	Reduce: for each key-value pair, we compair each two key-value pairs and if user_id_a != user_id_b, and |time_shift_a - time_shift_b| < 2, we output <timestampe + timeshift_a, user_id_a, user_id_b> if user_id_a < user_id_b and vice versa.

Usage:

	cs511_data is in hadoop fs /usr/root/mp1t2/input

	javac -cp /usr/hdp/2.4.0.0-169/hadoop/*:/usr/hdp/2.4.0.0-169/hadoop-mapreduce/* mp1t2.java
	jar -cvf mp1t2.jar *.class
	hadoop jar mp1t2.jar mp1t2 mp1t2/input/cs511_data mp1t2/output

Running time: 6 min

Methods:
	map: 
		intput: each line of data
		output: <campaign id, [count of impressions, count of clicks, count of conversions]>

	reduce:
		input: <campaign id, [count of impressions, count of clicks, count of conversions]>
		output <campaign id, [sum of impressions, sum of clicks, sum of conversions]>

Usage: 

	cs511_data is in hadoop fs /usr/root/mp1t1/input

	javac -cp /usr/hdp/2.4.0.0-169/hadoop/*:/usr/hdp/2.4.0.0-169/hadoop-mapreduce/* mp1t1.java
	jar -cvf mp1t1.jar *.class
	hadoop jar mp1t1.jar mp1t1 mp1t1/input/cs511_data mp1t1/output

Running time: 2 min 
