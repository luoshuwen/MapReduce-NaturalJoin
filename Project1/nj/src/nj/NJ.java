package nj;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.ArrayList;

public class NJ {

   public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, IntWritable, Text> {
     public void map(LongWritable key, Text value, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException {
    	 
       String line = value.toString();
       
       String ii=((FileSplit)reporter.getInputSplit()).getPath().getName().toString();

       String i=(ii.indexOf("S")==-1?"1":"2");//r=1
       
       int vother=0;
       int kkb=0;
       
       Text word = new Text();
       StringTokenizer tokenizer = new StringTokenizer(line);
       
       int j=0;
       while (tokenizer.hasMoreTokens()) {    	    	 
         word.set(tokenizer.nextToken());         
         if(j==0 && i=="1"){//first of R        	
        	 vother=Integer.parseInt(word.toString().trim()) ;//get a
        	 j++;
        	 continue;
         }
         if(i=="1"&& j!=0){//r b
        	 kkb= Integer.parseInt(word.toString().trim());//get b
        	 IntWritable key1=new IntWritable(kkb%31); 
        	 Text value1=new Text(     Integer.toString(kkb/31).trim()    +" "+ i.trim() +" " + Integer.toString(vother).trim()      );
        	 output.collect(key1 ,value1);
         }
         if(j==0&&i!="1"){//s b
        	 kkb= Integer.parseInt(word.toString().trim());//get b
        	 j++;
        	 continue;
         }
         if(j!=0 && i!="1"){//s c   
        	 vother=Integer.parseInt(word.toString().trim()) ;//get c
        	 IntWritable key1=new IntWritable(kkb%31);        	 
        	 Text value1=new Text(   Integer.toString(kkb/31).trim()+" "+i.trim()+" "+Integer.toString(vother).trim());
        	 output.collect( key1 ,value1);
         } 
       }       
     }
   }
   
   public static class Reduce extends MapReduceBase implements Reducer<IntWritable,Text, Text, Text> {
     public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	 ArrayList<String> al=new ArrayList();
    	 
    	 while (values.hasNext()) {
    		 String record = values.next().toString();
    		 String[] rrecord=new String[3];
    		 String s1="";
    		 
    		 StringTokenizer tokenizer = new StringTokenizer(record);
             int ii=0;
    		 while (tokenizer.hasMoreTokens()) {
    			 rrecord[ii++]=tokenizer.nextToken(); 
    		 }
    		 
    		 int flag=0;
    		 
    		 for(int qq=0;qq<al.size();qq++){
    			 s1=al.get(qq).trim().split(" |\\t|\\n")[0];
    			 if(Integer.parseInt(s1)==Integer.parseInt(rrecord[0])){
    				 flag=1;  
    				 al.set(qq, al.get(qq).trim()+" "+rrecord[1].trim()+" "+rrecord[2].trim());
    				 break;
    			 }
    		 }    		    		 
    		 if(flag==0)
    			 al.add(record);    		 
    	 }

         for(int qq=0;qq<al.size();qq++){
      	   ArrayList<String> arr=new ArrayList();
      	   for(int mm=0;mm<al.get(qq).trim().split(" |\\t|\\n").length;mm++){
      		 if(al.get(qq).trim().split(" |\\t|\\n")[mm].length()!=0)
      			 arr.add(al.get(qq).trim().split(" |\\t|\\n")[mm]);
      	   }
      	 
      	   for(int ii=1;ii<arr.size()-1;ii=ii+2){
      		   if(Integer.parseInt(arr.get(ii))==1){//R
      			   for(int jj=ii+2;jj<arr.size()-1;jj+=2){
      				   if(Integer.parseInt(arr.get(ii))!=Integer.parseInt(arr.get(jj))){
      					 output.collect( null,new Text(   "("+   arr.get(ii+1).trim()+"," +  Integer.toString((key.get()+31*Integer.parseInt(arr.get(0)))) +","+    arr.get(jj+1).trim() +")"  ));
      				   }
      			   }
      		   }else{//S
      			 for(int jj=ii+2;jj<arr.size()-1;jj+=2){
    				   if(Integer.parseInt(arr.get(ii))!=Integer.parseInt(arr.get(jj))){
    					 output.collect(null,new Text(   "("+   arr.get(jj+1).trim()+"," +  Integer.toString((key.get()+31*Integer.parseInt(arr.get(0)))) +","+    arr.get(ii+1).trim() +")"  ));
    				   }
    			   }
      	    	}
      		   }
         }
     }
   }

   public static void main(String[] args) throws Exception {
     JobConf conf = new JobConf(NJ.class);
     conf.setJobName("nj");

     conf.setNumReduceTasks(32);
     
     conf.setOutputKeyClass(IntWritable.class);
     conf.setOutputValueClass(Text.class);


     conf.setMapperClass(Map.class);

     conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     JobClient.runJob(conf);
   }
}

