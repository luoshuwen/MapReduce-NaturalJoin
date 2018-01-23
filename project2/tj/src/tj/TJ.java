package tj;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.ArrayList;

public class TJ {

	
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

		       int vv=0,vother=0,vother2=0;
		       int kkb=0;
		       
		       Text word = new Text();
		       StringTokenizer tokenizer = new StringTokenizer(line);
		       
		       int j=0;
		       
		       while (tokenizer.hasMoreTokens()) {    	    	 
		         word.set(tokenizer.nextToken());  
		         
		         if(j==0){      	
		        	 vv=Integer.parseInt(word.toString().trim()) ;//get A
		        	 j++;
		        	 continue;
		         }
		         else{
		        	 vother=Integer.parseInt(word.toString().trim()) ;//get B
		         }
		       } 
		       
		       if(j!=0){
			       kkb=Math.abs(vother) %5;
			       Text value1=new Text(i.trim()+" "+Integer.toString(vv).trim()+" "+Integer.toString(vother).trim());
			       
			       if(i=="0"){
			    	   for(int mm=0;mm<5;mm++){
				    	   for(int nn=0;nn<5;nn++){
				    		   Text key1=new Text(Integer.toString(kkb).trim()+" "+Integer.toString(mm).trim()+" "+Integer.toString(nn).trim());
				    		   output.collect( key1 ,value1);
				    	   }
				       }
			       }
	               if(i=="1"){
			    	   for(int mm=0;mm<5;mm++){
				    	   for(int nn=0;nn<5;nn++){
				    		   Text key1=new Text(Integer.toString(mm).trim()+" "+Integer.toString(kkb).trim()+" "+Integer.toString(nn).trim());
				    		   output.collect( key1 ,value1);
				    	   }
				       }
			       }
	               if(i=="2"){
			    	   for(int mm=0;mm<5;mm++){
				    	   for(int nn=0;nn<5;nn++){
				    		   Text key1=new Text(Integer.toString(mm).trim()+" "+Integer.toString(nn).trim()+" "+Integer.toString(kkb).trim());
				    		   output.collect( key1 ,value1);
				    	   }
				       }	   
	               }
		       }

		       		             
		     }
		   }
		   
		   public static class Reduce extends MapReduceBase implements Reducer<Text,Text, Text, Text> {
		     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

//		    	 while (values.hasNext()) {
//		    		 String record = values.next().toString();
//		    		 output.collect(key,new Text("~QWQ~"+record));
//		    	 }
		    	 
		    	 ArrayList<String> al=new ArrayList();
		    	 
		    	 while (values.hasNext()) {
		    		 String record = values.next().toString();
		    		 String[] rrecord=new String[3];		    		 
		    		 StringTokenizer tokenizer = new StringTokenizer(record);
		             int ii=0;
		    		 while (tokenizer.hasMoreTokens()) {
		    			 rrecord[ii++]=tokenizer.nextToken(); 
		    		 }
		    		 
		    		 //output.collect(key,new Text(rrecord[0].trim()+"~QWQ~"+rrecord[1].trim()+"~QWQ~"+rrecord[2].trim()));
		    		 
                     String ss=rrecord[0].trim()+" "+rrecord[1].trim()+" "+rrecord[2].trim();
                     al.add(ss);
		    	 }
 
		      for(int mm=0;mm<al.size();mm++){
		    	  if(Integer.parseInt(al.get(mm).trim().split(" |\\t|\\n")[0].trim())==Integer.parseInt("0")){
		    		  //output.collect(key,new Text(al.get(mm).trim().split(" |\\t|\\n")[0].trim()+"~QWQ~"+al.get(mm).trim().split(" |\\t|\\n")[1].trim()+"~QWQ~"+al.get(mm).trim().split(" |\\t|\\n")[2].trim() ));
		    		  for(int nn=0;nn<al.size();nn++){
		    			  if(  (Integer.parseInt(al.get(mm).trim().split(" |\\t|\\n")[2].trim())==Integer.parseInt(al.get(nn).trim().split(" |\\t|\\n")[1].trim())) && (Integer.parseInt(al.get(nn).trim().split(" |\\t|\\n")[0].trim())==Integer.parseInt("1"))   ){
		    				  for(int qq=0;qq<al.size();qq++){
		    					  if(          (Integer.parseInt(al.get(qq).trim().split(" |\\t|\\n")[0].trim())==Integer.parseInt("2"))        &&       (Integer.parseInt(al.get(qq).trim().split(" |\\t|\\n")[1].trim())==Integer.parseInt(al.get(nn).trim().split(" |\\t|\\n")[2].trim()))          &&          (Integer.parseInt(al.get(qq).trim().split(" |\\t|\\n")[2].trim())==Integer.parseInt(al.get(mm).trim().split(" |\\t|\\n")[1].trim()))          ){
		    						  Text keyy=new Text("("+al.get(mm).trim().split(" |\\t|\\n")[1]+","+al.get(mm).trim().split(" |\\t|\\n")[2]+","+al.get(nn).trim().split(" |\\t|\\n")[2]+")");
		    						  output.collect(null,keyy);
		    					  }
		    				  }
		    			  }
		    		  }
		    		  
		    	  }		    		
		      }
		      
		      
		      
		     }
		   }

		   public static void main(String[] args) throws Exception {
			  long starTime=System.currentTimeMillis();
		     JobConf conf = new JobConf(TJ.class);
		     conf.setJobName("tj");

		     //conf.setNumReduceTasks(128);
		     
		     conf.setOutputKeyClass(Text.class);
		     conf.setOutputValueClass(Text.class);


		     conf.setMapperClass(Map.class);

		     conf.setReducerClass(Reduce.class);

		     conf.setInputFormat(TextInputFormat.class);
		     conf.setOutputFormat(TextOutputFormat.class);

		     FileInputFormat.setInputPaths(conf, new Path(args[0]));
		     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		     JobClient.runJob(conf);
		     long endTime=System.currentTimeMillis();
			 long Time=endTime-starTime;
		     System.out.println(Time);
		   }
	
}


