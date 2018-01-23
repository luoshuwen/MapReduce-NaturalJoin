package tj3;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.ArrayList;

public class TJ3 {

	   public static int flag=0;
	   public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, Text,Text> {
		     public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		    	 
		       String line = value.toString();
		       
		       String ii=((FileSplit)reporter.getInputSplit()).getPath().getName().toString();

		       String i="";
		       if(ii.indexOf("R")!=-1){
		    	   i="0";   
		       }
		       if(ii.indexOf("S")!=-1){
		    	   i="1";   
		       }
		       if(ii.indexOf("T")!=-1){
		    	   i="2";   
		       }

		       output.collect( new Text("1") ,new Text(i.trim()+" "+line.trim()));

		       		             
		     }
		   }
		   
		   public static class Reduce extends MapReduceBase implements Reducer<Text,Text, Text, Text> {
		     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

//		    	 while (values.hasNext()) {
//		    		 String record = values.next().toString();
//		    		 output.collect(null,new Text(record));
//		    	 }
		    	 
		    	 ArrayList<String> al0=new ArrayList();
		    	 ArrayList<String> al1=new ArrayList();
		    	 ArrayList<String> al2=new ArrayList();
		    	 
		    	 while (values.hasNext()) {
		    		 String record = values.next().toString();
		    		 String[] rrecord=new String[3];		    		 
		    		 StringTokenizer tokenizer = new StringTokenizer(record);
		             int ii=0;
		    		 while (tokenizer.hasMoreTokens()) {
		    			 rrecord[ii++]=tokenizer.nextToken(); 
		    		 }
		    		 if(ii==3){
		    			 if(Integer.parseInt(rrecord[0])==Integer.parseInt("0")){
		    				 al0.add(rrecord[1].trim()+" "+rrecord[2].trim());
		    			 }
		    			 if(Integer.parseInt(rrecord[0])==Integer.parseInt("1")){
		    				 al1.add(rrecord[1].trim()+" "+rrecord[2].trim());
		    			 }
		    			 if(Integer.parseInt(rrecord[0])==Integer.parseInt("2")){
		    				 al2.add(rrecord[1].trim()+" "+rrecord[2].trim());
		    			 }
		    		 }
		    	 }
		    	 
		    	 int p0=0,p1=0,p2=0;
		    	 for(int i=0;i<al0.size();i++){
		    		 for(int j=0;j<al1.size();j++){
		    			 if(Integer.parseInt(al0.get(i).trim().split(" |\\t|\\n")[1]) ==Integer.parseInt( al1.get(j).trim().split(" |\\t|\\n")[0])){
		    				 p0++;
		    				 //output.collect(null,new Text(al0.get(i).trim().split(" |\\t|\\n")[1]+"~Q~"+al1.get(j).trim().split(" |\\t|\\n")[0]+"~Q~"+Integer.toString(p0)));
		    			 }
		    		 }
		    	 }
		    	 p0=p0+al2.size();
		    	 for(int i=0;i<al1.size();i++){
		    		 for(int j=0;j<al2.size();j++){
		    			 if(Integer.parseInt(al1.get(i).trim().split(" |\\t|\\n")[1])==Integer.parseInt(al2.get(j).trim().split(" |\\t|\\n")[0])){
		    				 p1++;
		    			 }
		    		 }
		    	 }
		    	 p1=p1+al0.size();
		    	 for(int i=0;i<al2.size();i++){
		    		 for(int j=0;j<al0.size();j++){
		    			 if(Integer.parseInt(al2.get(i).trim().split(" |\\t|\\n")[1])==Integer.parseInt(al0.get(j).trim().split(" |\\t|\\n")[0])){
		    				 p2++;
		    			 }
		    		 }
		    	 }
		    	 p2=p2+al1.size();
		    	 
		    	 if(p0<=p1&&p0<=p2){
		    		 for(int i=0;i<al0.size();i++){
		    			 for(int j=0;j<al1.size();j++){
		    				 if(Integer.parseInt(al0.get(i).trim().split(" |\\t|\\n")[1]) ==Integer.parseInt( al1.get(j).trim().split(" |\\t|\\n")[0])){
		    					 output.collect(null,new Text(al0.get(i).trim().split(" |\\t|\\n")[0].trim()+"\t"+al0.get(i).trim().split(" |\\t|\\n")[1].trim()+"\t"+al1.get(j).trim().split(" |\\t|\\n")[1].trim()));
		    				 }
		    			 }
		    		 }
		    		 for(int i=0;i<al2.size();i++){
		    			 output.collect(null,new Text(al2.get(i).trim().split(" |\\t|\\n")[0].trim()+"\t"+al2.get(i).trim().split(" |\\t|\\n")[1].trim()));
		    		 }
		    	 }
		    	 else{
			    	 if(p1<=p0&&p1<=p2){
			    		 for(int i=0;i<al1.size();i++){
			    			 for(int j=0;j<al2.size();j++){
			    				 if(Integer.parseInt(al1.get(i).trim().split(" |\\t|\\n")[1]) ==Integer.parseInt( al2.get(j).trim().split(" |\\t|\\n")[0])){
			    					 output.collect(null,new Text(al1.get(i).trim().split(" |\\t|\\n")[0].trim()+"\t"+al1.get(i).trim().split(" |\\t|\\n")[1].trim()+"\t"+al2.get(j).trim().split(" |\\t|\\n")[1].trim()));
			    				 }
			    			 }
			    		 }
			    		 for(int i=0;i<al0.size();i++){
			    			 output.collect(null,new Text(al0.get(i).trim().split(" |\\t|\\n")[0].trim()+"\t"+al0.get(i).trim().split(" |\\t|\\n")[1].trim()));
			    		 }
			    		 flag=1;
			    	 }
			    	 else{
				    	 if(p2<=p0&&p2<=p1){
				    		// System.out.println("~~~~~~~");
				    		 for(int i=0;i<al2.size();i++){
				    			 for(int j=0;j<al0.size();j++){
				    				 if(Integer.parseInt(al2.get(i).trim().split(" |\\t|\\n")[1]) ==Integer.parseInt( al0.get(j).trim().split(" |\\t|\\n")[0])){
				    					 output.collect(null,new Text(al2.get(i).trim().split(" |\\t|\\n")[0].trim()+"\t"+al2.get(i).trim().split(" |\\t|\\n")[1].trim()+"\t"+al0.get(j).trim().split(" |\\t|\\n")[1].trim()));
				    				 }
				    			 }
				    		 }
				    		 for(int i=0;i<al1.size();i++){
				    			 output.collect(null,new Text(al1.get(i).trim().split(" |\\t|\\n")[0].trim()+"\t"+al1.get(i).trim().split(" |\\t|\\n")[1].trim()));
				    		 }
				    		 flag=2;
				    	 }
			    	 }

		    	 }


		     }
		   }

		   public static class Map2 extends MapReduceBase implements Mapper <LongWritable, Text, Text,Text> {
			     public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			    	 String line = value.toString();
			    	 output.collect(new Text("1"),new Text(line));
				  
			       }
		   }
		   
		   public static class Reduce2 extends MapReduceBase implements Reducer<Text,Text, Text, Text> {
			     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			    	 ArrayList<String> al0=new ArrayList();
			    	 ArrayList<String> al1=new ArrayList();
			    	 
			    	 while (values.hasNext()) {
			    		 String record = values.next().toString();
			    		 String[] rrecord=new String[3];		    		 
			    		 StringTokenizer tokenizer = new StringTokenizer(record);
			             int ii=0;
			    		 while (tokenizer.hasMoreTokens()) {
			    			 rrecord[ii++]=tokenizer.nextToken(); 
			    		 }
			    		 if(ii==3){
			    			 al0.add(rrecord[0].trim()+" "+rrecord[1].trim()+" "+rrecord[2].trim());
			    		 }
			    		 else{
			    			 al1.add(rrecord[0].trim()+" "+rrecord[1].trim());
			    		 }
			    	 }
			    	 
			    	 for(int i=0;i<al0.size();i++){
			    		 for(int j=0;j<al1.size();j++){
			    			 if((  Integer.parseInt(al0.get(i).trim().split(" |\\t|\\n")[0].trim()) == Integer.parseInt(al1.get(j).trim().split(" |\\t|\\n")[1].trim()) )&&(      Integer.parseInt(al0.get(i).trim().split(" |\\t|\\n")[2].trim())  == Integer.parseInt(al1.get(j).trim().split(" |\\t|\\n")[0].trim())     )){
			    				 if(flag==0){
			    					 output.collect(null,new Text("("+al0.get(i).trim().split(" |\\t|\\n")[0].trim()+","+al0.get(i).trim().split(" |\\t|\\n")[1].trim()+","+al0.get(i).trim().split(" |\\t|\\n")[2].trim()+")"));
			    				 }
			    				 if(flag==1){
			    					 output.collect(null,new Text("("+al0.get(i).trim().split(" |\\t|\\n")[2].trim()+","+al0.get(i).trim().split(" |\\t|\\n")[0].trim()+","+al0.get(i).trim().split(" |\\t|\\n")[1].trim()+")"));
			    				 }
			    				 if(flag==2){
			    					 output.collect(null,new Text("("+al0.get(i).trim().split(" |\\t|\\n")[1].trim()+","+al0.get(i).trim().split(" |\\t|\\n")[2].trim()+","+al0.get(i).trim().split(" |\\t|\\n")[0].trim()+")"));
			    				 }
			    			 }
			    		 }
			    	 }
                   

			    	 
			     }
			   }
		   
		   public static void main(String[] args) throws Exception {
			   
			 long starTime=System.currentTimeMillis();
			  
		     JobConf conf = new JobConf(TJ3.class);
		     conf.setJobName("tj3");

		     //conf.setNumReduceTasks(2);
		     
		     conf.setOutputKeyClass(Text.class);
		     conf.setOutputValueClass(Text.class);


		     conf.setMapperClass(Map.class);

		     conf.setReducerClass(Reduce.class);

		     conf.setInputFormat(TextInputFormat.class);
		     conf.setOutputFormat(TextOutputFormat.class);

		     FileInputFormat.setInputPaths(conf, new Path(args[0]));
		     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		     JobClient.runJob(conf);
		     
		     
		     
		     
		     JobConf conf2 = new JobConf(TJ3.class);
		     conf2.setJobName("tj32");

		     //conf2.setNumReduceTasks(128);
		     
		     conf2.setOutputKeyClass(Text.class);
		     conf2.setOutputValueClass(Text.class);


		     conf2.setMapperClass(Map2.class);

		     conf2.setReducerClass(Reduce2.class);

		     conf2.setInputFormat(TextInputFormat.class);
		     conf2.setOutputFormat(TextOutputFormat.class);

		     FileInputFormat.setInputPaths(conf2, new Path(args[1]));
		     FileOutputFormat.setOutputPath(conf2, new Path(args[2]));

		     JobClient.runJob(conf2);

		     
		     
		     
		     
		     
		     long endTime=System.currentTimeMillis();
			 long Time=endTime-starTime;
		     System.out.println(Time);
		     //System.out.println(flag);
		   }
	
}
