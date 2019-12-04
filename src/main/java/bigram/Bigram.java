package bigram;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;
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

public class Bigram {

  private static int topN = 10;

  public static class BigramMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable mapValue = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();

      if (line != null && line.length() > 0) {
        // 去掉标点符号 \\d替换掉数字
        line = line.replaceAll("[,.()*'\\[\\]!?:;\"\\{\\}/%\\|@<>#\\$\\d&~_]", "");
        // - 替换为空格
        line = line.replaceAll("-", " ");
        String[] words = line.split(" ");
        for (String word : words) {
          //去除空格
          if (" ".equals(word)) {
            continue;
          }
          //如果单词长度为1， 是否算入？
          word = word.trim();
          if (word.length() == 1) {
            context.write(new Text(word), mapValue);
          }
          //分解出bigram
          for (int i = 0; i < word.length() - 1; i++) {
            char[] chars = word.toCharArray();
            String bigram = chars[i] + "" + chars[i + 1];
            context.write(new Text(bigram), mapValue);
          }
        }
      }


    }
  }

  public static class BigramReducer extends
      Reducer<Text, IntWritable, NullWritable, Tuple2<String, Integer>> {

    private IntWritable reduceValue = new IntWritable();

    private TreeSet<Tuple2<String, Integer>> treeSet = new TreeSet<Tuple2<String, Integer>>(
        new Comparator<Tuple2<String, Integer>>() {
          @Override
          public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o2.f1 - o1.f1;
          }
        });

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      List<IntWritable> list = Lists.newArrayList(values);
      int sum = 0;
      for (IntWritable reduceValue : list) {
        sum += reduceValue.get();
      }
      if (treeSet.size() < topN) {
        treeSet.add(new Tuple2<>(key.toString(), sum));
      } else if (sum > treeSet.first().f1) {
        treeSet.pollLast();
        treeSet.add(new Tuple2<>(key.toString(), sum));
      }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Tuple2<String, Integer> tuple2 : treeSet) {
        context.write(NullWritable.get(), tuple2);
      }
    }
  }

  public static class Tuple2<K, V> {

    private K f0;
    private V f1;

    public Tuple2() {
    }

    public Tuple2(K f0, V f1) {
      this.f0 = f0;
      this.f1 = f1;
    }

    public K getF0() {
      return f0;
    }

    public void setF0(K f0) {
      this.f0 = f0;
    }

    public V getF1() {
      return f1;
    }

    public void setF1(V f1) {
      this.f1 = f1;
    }

    @Override
    public String toString() {
      return f0 + "\t" +f1;
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    //模拟传参 写入本地， hdfs， 则加入hdfs配置并使用hdfs路径
    args = new String[2];
    args[0] = "file:///Users/t1mon/sources/bigram/data/pg100.txt";
    args[1] = "file:///Users/t1mon/sources/bigram/data/output/bigram/".concat(new Date().getTime() + "");

    //从hdfs读取并写入hdfs， 则加入hdfs配置(resource下的两份文件替换成你的文件)并使用hdfs路径
//    args[0] = "/tmp/data/pg100.txt";
//    args[1] = "/tmp/output/".concat(new Date().getTime() + "");


    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    job.setJarByClass(Bigram.class);

    job.setMapperClass(BigramMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));

    job.setReducerClass(BigramReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

  }


}
