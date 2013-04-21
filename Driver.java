package wiki_tagging;

/* create user tags
 * 执行：hadoop xx.jar HDFSFileName.input HDFSFileName.output word_conceptlists_file
 * 输入：hdfs-- id
 * 输出：IDi  id_tag
 * 描述：目的--打印用户的标签
 */

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;






public class Driver {

    public static boolean checkAndDelete(final String path, Configuration conf) {
        Path dst_path = new Path(path);
        try {

                FileSystem hdfs = dst_path.getFileSystem(conf);

                if (hdfs.exists(dst_path)) {

                        hdfs.delete(dst_path, true);
                }
        } catch (IOException e) {
                e.printStackTrace();
                return false;
        }
        return true;

}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: toCreateTags <in> <out> <word_conceptlists_file>");
      System.exit(2);
    }
    
    for(int i=2;i<otherArgs.length; i++)
      DistributedCache.addCacheFile((URI.create(otherArgs[i])), conf);
 
    Job job = new Job(conf, "Create Tags");
    
    checkAndDelete(otherArgs[1],conf);
    job.setJarByClass(Driver.class);
    job.setMapperClass(wiki_mapper.class);
    job.setReducerClass(wiki_reducer.class);          //
    job.setMapOutputKeyClass(Text.class);  
    job.setMapOutputValueClass(Text.class); 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
 //   HDFSTool.checkAndDelete(otherArgs[1], conf);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    job.setNumReduceTasks(5);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
      
  }
}
