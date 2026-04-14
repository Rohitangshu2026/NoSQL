import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

/**
 * A MapReduce job to generate the co-occurring word matrix using the Pairs
 * approach,
 * without any explicit local aggregation.
 */
public class PairsCoOccurrence extends Configured implements Tool {
    /**
     * Emits a pair of words and a count of one for each co-occurrence within the
     * specified contextual distance.
     */
    public static class PairMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();

        private Set<String> top50 = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    BufferedReader reader = new BufferedReader(
                            new FileReader(new File(new org.apache.hadoop.fs.Path(uri.getPath()).getName())));
                    String line;

                    while ((line = reader.readLine()) != null) {
                        String word = line.split("\\s+")[0];
                        top50.add(word.toLowerCase());
                    }

                    reader.close();
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            line = line.replaceAll("&[a-z]+;", "");
            line = line.replaceAll("<[^>]+>", "");
            line = line.replaceAll("(https?://|www\\.)\\S+", "");
            line = line.replaceAll("\\[\\[[^\\]]+\\]\\]", "");
            String[] tokens = line.split("[^\\w']+");

            // Build dense list of ALL valid tokens (not just top-50)
            List<String> validWords = new ArrayList<>();
            for (String token : tokens) {
                token = token.toLowerCase();
                if (token.isEmpty())
                    continue;
                if (token.length() < 2)
                    continue;
                validWords.add(token);
            }

            int d = context.getConfiguration().getInt("cooccur.distance", 1);

            for (int i = 0; i < validWords.size(); i++) {
                String center = validWords.get(i);
                if (!top50.contains(center))
                    continue;

                int start = Math.max(0, i - d);
                int end = Math.min(validWords.size() - 1, i + d);
                for (int j = start; j <= end; j++) {
                    if (j == i)
                        continue;
                    String neighbor = validWords.get(j);
                    if (!top50.contains(neighbor))
                        continue;

                    pair.set("(" + center + "," + neighbor + ")");
                    context.write(pair, one);
                }
            }
        }
    }

    /**
     * Aggregates the local and global counts for each word pair to compute the
     * final co-occurrence frequency.
     */
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String topWordsFile = null;

        List<String> remainingArgs = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("-topwords".equals(args[i])) {
                topWordsFile = args[++i];
            } else {
                remainingArgs.add(args[i]);
            }
        }

        if (remainingArgs.size() < 2) {
            System.err.println("Usage: PairsCoOccurrence [-D cooccur.distance=<d>] " +
                    "-topwords <top50words.txt> <input> <output>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Pairs Co-Occurrence d=" +
                conf.getInt("cooccur.distance", 1));
        job.setJarByClass(PairsCoOccurrence.class);

        // job.setInputFormatClass(CustomFileInputFormat.class);
        // org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
        // .setMaxInputSplitSize(job, 134217728);

        job.setMapperClass(PairMapper.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (topWordsFile != null)
            job.addCacheFile(new Path(topWordsFile).toUri());

        FileInputFormat.setMaxInputSplitSize(job, 33554432L);
        FileInputFormat.addInputPath(job, new Path(remainingArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs.get(1)));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PairsCoOccurrence(), args);
        System.exit(res);
    }
}