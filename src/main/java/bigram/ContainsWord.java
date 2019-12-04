package bigram;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ContainsWord {

  public static class ContainsWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable mapValue = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      if (line != null && line.length() > 0) {
        //  \\d替换掉数字
        line = line.replaceAll("[,.()*'\\[\\]!?:;\"\\{\\}/%\\|@<>#\\$\\d&~_]", "");
        // - 替换为空格
        line = line.replaceAll("-", " ");
        if (line.contains("torture")) {
          context.write(new Text(line.trim()), mapValue);
        }
      }
    }
  }


  public static class ContainsWordReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      context.write(NullWritable.get(), key);

    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    //模拟传参 写入本地， hdfs， 则加入hdfs配置并使用hdfs路径
    args = new String[2];
    args[0] = "/Users/mac/Documents/workspace/bigram/src/main/resources/pg100.txt";
    args[1] = "/Users/mac/Documents/workspace/bigram/src/main/resources/contains-word/".concat(new Date().getTime() + "");

    //从hdfs读取并写入hdfs， 则加入hdfs配置(resource下的两份文件替换成你的文件)并使用hdfs路径
//    args[0] = "/tmp/data/pg100.txt";
//    args[1] = "/tmp/output/contains-word/".concat(new Date().getTime() + "");

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    job.setJarByClass(ContainsWord.class);

    job.setMapperClass(ContainsWordMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));

    job.setReducerClass(ContainsWordReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

  }
}
