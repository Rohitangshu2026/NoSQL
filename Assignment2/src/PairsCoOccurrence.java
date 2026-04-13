import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class PairsCoOccurrence {
    public static class PairMapper extends Mapper<Object,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();

        private Set<String> top50 = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if(cacheFiles != null){
                for(URI uri : cacheFiles){
                    BufferedReader reader = new BufferedReader(new FileReader(new File(new org.apache.hadoop.fs.Path(uri.getPath()).getName())));
                    String line;

                    while((line = reader.readLine()) != null) {
                        String word = line.split("\\s+")[0];
                        top50.add(word);
                    }

                    reader.close();
                }
            }
        }

        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String line = value.toString().toLowerCase();
            String[] tokens = line.split("[^a-zA-Z']+");
            List<String> words = new ArrayList<>();

            for(String token : tokens){
                if(token.isEmpty()) continue;

                if(!top50.contains(token)) continue;

                words.add(token);
            }

            int d = context.getConfiguration().getInt("distance",1);

            for(int i = 0;i < words.size();i++){
                for(int j = i-d;j <= i+d;j++){
                    if(j < 0 || j >= words.size() || i == j) continue;
                    String pairStr = words.get(i) + "," + words.get(j);
                    pair.set(pairStr);
                    context.write(pair, one);
                }
            }
        }
    }

    public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int sum = 0;

            for(IntWritable val : values){
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Pair CoOccurrence");

        job.setJarByClass(PairsCoOccurrence.class);
        job.setMapperClass(PairMapper.class);
        job.setReducerClass(ReduceClass.class);

        job.setCombinerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new Path(args[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}