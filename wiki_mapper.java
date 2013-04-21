package wiki_tagging;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.splitWord.Analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.util.Bytes;




public class wiki_mapper extends Mapper<Object, Text, Text, Text>{

  private Text keyword = new Text();
	private String start_time = new String("201301");
	private String end_time = new String("201302");
	private ArrayList onemonth_content = new ArrayList();
	private String onemonth_tags = "";
	private StringBuffer tags = new StringBuffer();
    private static Configuration conf=null;  
    private static HTable table;
    private Hashtable<String,String[]> conlists_ht = new Hashtable();
    private Hashtable<String,Integer> concepts_vals = new Hashtable();
    static{  
        conf = HBaseConfiguration.create();  
        conf.set("hbase.zookeeper.quorum", "master,slave01,slave02");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
                        table=new HTable(conf,"Sina_Weibo_Content");
                } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }
    }  
    
        //   @SuppressWarnings("deprecation")
        public ArrayList scaneByStartEndKeys(String rowkey, HTable table, String ts, String te) { 
        	// for test:
        	//String time = t;
        	Scan scan = new Scan();
        	scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ct"));
        	scan.setStartRow(Bytes.toBytes(rowkey+"-"+ts));
        	scan.setStopRow(Bytes.toBytes(rowkey+"-"+te)); 
            ArrayList notags = new ArrayList();
            ArrayList paser = new ArrayList();
        	try{
        		ResultScanner rs = table.getScanner(scan);
        	

                   try {  
                	   
                	   for (Result r = rs.next(); r != null; r = rs.next())
                	   {
                		   onemonth_tags = new String(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("ct"))) + onemonth_tags + " ";
                		   //System.out.println(r.size()+"%%%%%%%%%%%%%%%%%%%%");
                	   }
                	   //return onemonth_tags;
                	   if (!onemonth_tags.equals(""))
                	   {
                		   
                		   Analysis udf = new ToAnalysis(new StringReader(onemonth_tags));
                		   Term term = null;
                		   //StringBuffer tagsbuf = new StringBuffer("");
                		   //String tags = new String("")               		   
                		   while((term=udf.next())!=null)
                		   {
                			   //tagsbuf.append(" ").append(term.getName());
                			   paser.add(term.getName());
                		   }
                		   //return tagsbuf.toString();   
                		   
                		   
                		   //ArrayList paser = ToAnalysis.paser(onemonth_tags);
                		   return paser;   
                		   
                	   }
                        
                   } 
                   catch (IOException e) {  
                         e.printStackTrace(); 
                   }
                rs.close();
                
        	}
        	catch(Exception e){
        		e.printStackTrace(); 
        		
        	}
         return notags; 
         }  

        
    	private void ClassifierSetup(Context context) throws IOException
    	{
    		
    		URI[] localFiles = DistributedCache.getCacheFiles(context.getConfiguration());
    		FileSystem fs = FileSystem.get(localFiles[0],context.getConfiguration());
    		BufferedReader conceptlists = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[0]))));
    		//BufferedReader normalizer = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[5]))));
    		//Predicates.init(linearModel, null, normalizer);
    		String line = null;
    		while((line = conceptlists.readLine())!=null)
    		{
    			String[] linelist = line.split("	");
    			if ((linelist.length == 2) && (linelist[1]!=null))
    			{
    			String[] conlist = linelist[1].split(",");
    			conlists_ht.put(linelist[0], conlist);
    			
    			}
    			
    		}
    		
    		conceptlists.close();
    		//normalizer.close();
    		
    	}
    	
    	
    	@Override
    	//IDF dictionary load and schema assemble
    	protected void setup(Context context) throws IOException
    	{
    		//SearchSetup(context);
    		ClassifierSetup(context);
    		
    	}
        
        public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {

//                      System.out.println(value.toString());
        	            
                        
                        onemonth_content = scaneByStartEndKeys(value.toString(),table,start_time,end_time);
                        if(onemonth_content.isEmpty())
                                return;
                        
                        for (int i = 0; i < onemonth_content.size();i++)
                        {
                        	if((conlists_ht.get(onemonth_content.get(i))) != null)
                        	{
                        		for(int j = 0; j < conlists_ht.get(onemonth_content.get(i)).length; j++)
                        		{
                        			if(conlists_ht.get(onemonth_content.get(i))[j] == null)
                        			{
                        				concepts_vals.put(conlists_ht.get(onemonth_content.get(i))[j], new Integer(1));
                        			}
                        			else
                        			{
                        				int t = concepts_vals.get(conlists_ht.get(onemonth_content.get(i))[j]);
                        				concepts_vals.put(conlists_ht.get(onemonth_content.get(i))[j], new Integer(t+1));
                        			}
                        		}
                        	}
						}
                        
                        for(Iterator itr = concepts_vals.keySet().iterator();itr.hasNext();)
                        {
                        	String concept = (String) itr.next();
                        	String val = concepts_vals.get(concept).toString();
                        	tags.append(concept).append(":").append(val).append(",");
                        	
                        }

                        
                        
                        
        //              System.out.println(value.toString()+" : "+tag);
                        keyword.set("sw-"+value.toString());
                        //System.out.println(onemonth_content+"%%%%%%%%%%%%%%%%%%%%");
                        context.write(keyword,new Text(tags.toString()));
                        //System.out.println(onemonth_content+"%%%%%%%%%%%%%%%%%%%%");
                        //keyword.clear();
                        
        }
}

