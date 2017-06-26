 package Cloud.Iliad;  
  
 import java.io.IOException;  
 import java.util.*;  
 import java.io.StringReader;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;  
 import org.apache.hadoop.conf.*;  
 import org.apache.hadoop.io.*;  
 import org.apache.hadoop.mapred.*;  
 import org.apache.hadoop.util.*;  
  
 public class MidtermExam2 {  
  
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text,Text> {  
      //private final static IntWritable one = new IntWritable(1);  
	  
	  
      	  
	  int target = 22;//!!!
	  int count = 0;
	 
      //private Text num1 = new Text();
	  private String n = "1";
	  static String line1="0";
	  static String line2="0";
	  //String score;
      public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {  
        count++;
		String line = value.toString(); 
		String[] tokens = line.split("	");
		String keyPart = tokens[0].toString();
		String valuePart = tokens[1].toString();  
		String[] tokensPart2 = valuePart.split(",");
		String gavalue = tokensPart2[0].toString(); 
		String value2 = tokensPart2[1].toString();
		//if(count == 1)target = Integer.valueOf(value2);
		
        //while (tokenizer.hasMoreTokens()) {  
           //output.collect(key, new Text(tokenizer.nextToken()));
        ///////////////////////////////select+mutate+crossOver////////////////////////////
        
		System.out.println("count = "+count);			
		if(count%2 == 1){
		line1 = (gavalue).toString();
		String decodeline1 = decodeChromo(line1);//decode the binary string
		score = scoreChromo(num.get(),decodeline1);
		
        System.out.println("line1 = "+score);
		if( Double.valueOf(score).toString()== Integer.toString(0)&&isValid(decodeline1)){
			output.collect(new Text(decodeline1), new Text(Integer.toString(count)));
		   Goon_Ornot = false;
		   return;
		}

		
		}else{ 
		line2 = (gavalue).toString(); 
		
		String decodeline2 = decodeChromo(line2);//decode the binary string
		score = scoreChromo(num.get(),decodeline2);
		System.out.println("line2 = "+score);
		if( Double.valueOf(score).toString() ==Integer.toString(0)&&isValid(decodeline2)){
			output.collect(new Text(decodeline2), new Text(Integer.toString(count)));
		    Goon_Ornot = false;
			return;
		}
		}
		
        if(line1!="0"&&line2!="0"){  
		System.out.println(line1+"!!!!!!"+line2);
		//crossOver(line1,line2);
		//mutate(line1);
		//mutate(line2);

        output.collect(new Text(sDouble.valueOf(core1+score2).toString()),new Text(line1+","+line2));
		line1="0";
		line2="0";
        //output.collect(new Text(keyPart),new Text(line2+","+gavalue));
		}  
      }  
    }
	
	
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {  
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {  
	  
		String sum = "";
        String n = "";			
        while (values.hasNext()) {  
		
		String line = values.next().toString(); 
		String[] tokens = line.split(",");
		String firstPart = tokens[0].toString();
		String secondPart = tokens[1].toString(); 
		
		cc1 = firstPart;
		cc2 = secondPart;
		crossOver();
		cc= cc1; 
		mutate();
		String decodeFirstPart = decodeChromo(cc);
		cc= cc2;
		mutate();
        String decodeSecondPart = decodeChromo(cc);
		
		//String decodeFirstPart = decodeChromo(firstPart);
		//String decodeSecondPart = decodeChromo(secondPart);

		double score1 = scoreChromo(22,decodeFirstPart);
		double total1 = total;
		double score2 = scoreChromo(22,decodeSecondPart);
		double total2 = total;
		
		//output.collect(new Text(Double.valueOf(rand.nextDouble()).toString()), new Text(firstPart+","+decodeFirstPart));//!!!some changes
        //output.collect(new Text(Double.valueOf(rand.nextDouble()).toString()), new Text(secondPart+","+decodeSecondPart));
	    
		 if((score1 ==0.0)&&isValid(decodeFirstPart)){
		 output.collect(new Text(Double.valueOf(score1).toString()), new Text(firstPart+","+decodeFirstPart+"."+Double.valueOf(total1).toString()));//!!!some changes 
		 Goon_Ornot = false;
		 return;}
		 if((score2 ==0.0)&&isValid(decodeSecondPart)){
		 output.collect(new Text(Double.valueOf(score2).toString()), new Text(secondPart+","+decodeSecondPart+"."+Double.valueOf(total2).toString()));//!!!some changes
		 Goon_Ornot = false;
		 return;}
		  
         output.collect(new Text(Double.valueOf(score1).toString()), new Text(firstPart+","+decodeFirstPart+"."+Double.valueOf(total1).toString()));
	     output.collect(new Text(Double.valueOf(score2).toString()), new Text(secondPart+","+decodeSecondPart+"."+Double.valueOf(total2).toString()));
	   }  
      }  
    }
	static public final boolean isValid(String c1) { 
		
			// Decode our chromo
			String decodedString = c1;
			
			boolean num = true;
			for (int x=0;x<decodedString.length();x++) {
				char ch = decodedString.charAt(x);

				// Did we follow the num-oper-num-oper-num patter
				if (num == !Character.isDigit(ch)) return false;
				
				// Don't allow divide by zero
				if (x>0 && ch=='0' && decodedString.charAt(x-1)=='/') return false;
				
				num = !num;
			}
			
			// Can't end in an operator
			if (!Character.isDigit(decodedString.charAt(decodedString.length()-1))) return false;
			
			return true;
		}
	
			// Decode the string
	static public final String decodeChromo(String cc1) {

        StringBuffer decodeChromo = new StringBuffer(chromoLen * 4);
        StringBuilder chromo = new StringBuilder(cc1);
			// Create a buffer
			decodeChromo.setLength(0);
			
			// Loop throught the chromo
			for (int x=0;x<chromo.length();x+=4) {
				// Get the
				int idx = Integer.parseInt(chromo.substring(x,x+4), 2);
				if (idx<ltable.length) decodeChromo.append(ltable[idx]);
			}
			
			// Return the string
			return decodeChromo.toString();
		}

    static public final double scoreChromo(int target,String line) {
			total = addUp(line);
			if (total == target) score = 0;
			
			return score = (double)1 / (target - total);
		}
	
	static public final int addUp(String line) { 
		
			// Decode our chromo
			String decodedString = line;
			
			// Total
			int tot = 0;
			
			// Find the first number
			int ptr = 0;
			while (ptr<decodedString.length()) { 
				char ch = decodedString.charAt(ptr);
				if (Character.isDigit(ch)) {
					tot=ch-'0';
					ptr++;
					break;
				} else {
					ptr++;
				}
			}
			
			// If no numbers found, return
			if (ptr==decodedString.length()) return 0;
			
			// Loop processing the rest
			boolean num = false;
			char oper=' ';
			while (ptr<decodedString.length()) {
				// Get the character
				char ch = decodedString.charAt(ptr);
				
				// Is it what we expect, if not - skip
				if (num && !Character.isDigit(ch)) {ptr++;continue;}
				if (!num && Character.isDigit(ch)) {ptr++;continue;}
			
				// Is it a number
				if (num) { 
					switch (oper) {
						case '+' : { tot+=(ch-'0'); break; }
						case '-' : { tot-=(ch-'0'); break; }
						case '*' : { tot*=(ch-'0'); break; }
						case '/' : { if (ch!='0') tot/=(ch-'0'); break; }
					}
				} else {
					oper = ch;
				}			
				
				// Go to next character
				ptr++;
				num=!num;
			}
			
			return tot;
		}	
		
		static String cc1;
		static String cc2;
		static String cc;

	static	public final void crossOver() {
            //StringBuffer chromo = new StringBuffer(chromoLen * 4);
			// Should we cross over?
			
			StringBuilder c1 = new StringBuilder(cc1);
			StringBuilder c2 = new StringBuilder(cc2);
			
			if (rand.nextDouble() > crossRate) return;
			
			// Generate a random position
			int pos = rand.nextInt(c1.length());
			
			// Swap all chars after that position
			for (int x=pos;x<c1.length();x++) {
				// Get our character
				char tmp = c1.charAt(x);
				
				// Swap the chars
				c1.setCharAt(x, c2.charAt(x));
				c2.setCharAt(x, tmp);
			}
			cc1 = c1.toString();
			cc2 = c2.toString();
		}
          
         // Mutation
	static	public final void mutate() {
		  StringBuilder cb = new StringBuilder(cc);
			for (int x=0;x<cb.length();x++) {
				if (rand.nextDouble()<=mutRate) 
					cb.setCharAt(x, (cb.charAt(x)=='0' ? '1' : '0'));
			}
			cc = cb.toString();
		}		  

	

		
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Static info
	static char[] ltable = {'0','1','2','3','4','5','6','7','8','9','+','-','*','/'};
	static int chromoLen = 5;
	static double crossRate = .7;
	static double mutRate = .001;
	static Random rand = new Random();
	static int poolSize = 40;	// Must be even
	static String result1 ;
	static String result2;
	static String result  ;
	static int target;
	static boolean Goon_Ornot = true;
	private IntWritable num = new IntWritable(22);
	static int total;
	static double score;

	
	
	public MidtermExam2(String in)throws Exception{ 
	  String tempDir=in;
	int i = 0;
	while(Goon_Ornot){
		i++;
      JobConf conf = new JobConf(MidtermExam2.class);  
      conf.setJobName("MidtermExam2");  
	  conf.setNumReduceTasks(5);
  
      conf.setOutputKeyClass(Text.class);  
      conf.setOutputValueClass(Text.class); 

        
  
      conf.setMapperClass(Map.class);  
      //conf.setCombinerClass(Reduce.class);  
      conf.setReducerClass(Reduce.class);  
  
      conf.setInputFormat(TextInputFormat.class);  
      conf.setOutputFormat(TextOutputFormat.class);  
	  
	    String path1="output_midtermExam/output_midtermExam2"+(i+1);	  
		Path outPath = new Path(path1);
		FileInputFormat.setInputPaths(conf, new Path(tempDir));
		FileOutputFormat.setOutputPath(conf, outPath);
		
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}
    
      JobClient.runJob(conf);  
	  tempDir=path1;
	}
	
    }
}
			