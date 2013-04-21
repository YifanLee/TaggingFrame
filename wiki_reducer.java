package wiki_tagging;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 


public class wiki_reducer 
extends Reducer<Text,Text,Text,Text> {
/*      private Matcher matcher=null;
        private String comtags = "简版|隐私|语音|短信|皮肤|条数|图片|触屏";
        private Pattern pattern=Pattern.compile(comtags);*/

//      StringBuffer result = new StringBuffer();
      Text onemonth_tags = new Text();
        public void reduce(Text key, Iterable<Text> values, 
                Context context
                ) throws IOException, InterruptedException {

/*              result.setLength(0);
                for (Text val : values) {
                        String str[] = val.toString().split(",");
                        for(int i=0;i<str.length;i++)
                        {
                                matcher=null;
                                matcher=pattern.matcher(str[i]);
                                if(matcher!=null && matcher.find())       //special user
                                {
                //                      System.out.println("delete value:"+str[i].toString());
                                        continue;
                                }
                                result.append(str[i].toString()+",");
                        }
                }
                context.write(key, new Text(result.toString().substring(0, result.toString().length()-1))); */
        	
        		for (Text val : values){
        			onemonth_tags = val;
        		}
                context.write(key, onemonth_tags);
                //onemonth_tags.clear();
        }
}
