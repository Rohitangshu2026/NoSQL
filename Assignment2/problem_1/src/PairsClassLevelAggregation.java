import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * A MapReduce job to generate the co-occurring word matrix using the Pairs approach
 * with class-level local aggregation.
 */
public class PairsClassLevelAggregation extends Configured implements Tool {

    // -----------------------------------------------------------------------
    // Mapper – in-mapper (map-CLASS-level) aggregation
    // -----------------------------------------------------------------------
    /**
     * Maintains an in-memory aggregation buffer across the entire task (class-level)
     * before emitting word pairs with their accumulated counts during the cleanup phase.
     */
    public static class PairsClassMapper extends Mapper<Object, Text, Text, IntWritable> {

        private int distance = 1;
        private Set<String> topWords = new HashSet<>();

        // Local aggregation buffer – lives for the entire mapper task lifetime
        private Map<String, Integer> localCounts = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            distance = conf.getInt("cooccur.distance", 1);

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    Path p = new Path(uri.getPath());
                    if (p.getName().equals("top50words.txt")) {
                        loadTopWords(p.getName());
                    }
                }
            }
        }

        private void loadTopWords(String fileName) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 1)
                        topWords.add(parts[0].toLowerCase());
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            line = line.replaceAll("&[a-z]+;", "");
            line = line.replaceAll("<[^>]+>", "");
            line = line.replaceAll("(https?://|www\\.)\\S+", "");
            line = line.replaceAll("\\[\\[[^\\]]+\\]\\]", "");
            String[] tokens = line.split("[^\\\\w']+");

            // Build dense list of ALL valid tokens (not just top-50)
            List<String> validWords = new ArrayList<>();
            for (String token : tokens) {
                if (token.isEmpty()) continue;
                if (token.length() < 2) continue;
                validWords.add(token);
            }

            for (int i = 0; i < validWords.size(); i++) {
                String center = validWords.get(i);
                if (!topWords.contains(center)) continue;

                int start = Math.max(0, i - distance);
                int end = Math.min(validWords.size() - 1, i + distance);

                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    String neighbor = validWords.get(j);
                    if (!topWords.contains(neighbor)) continue;

                    String pairKey = "(" + center + "," + neighbor + ")";
                    localCounts.merge(pairKey, 1, Integer::sum);
                }
            }
            // NOTE: context.write is NOT called inside map() – this is the key
            // difference from map-function-level aggregation.
        }

        /**
         * cleanup() is called ONCE after all map() calls for this mapper instance.
         * We flush the entire local buffer here.
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Text outKey = new Text();
            IntWritable outVal = new IntWritable();
            for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
                outKey.set(entry.getKey());
                outVal.set(entry.getValue());
                context.write(outKey, outVal);
            }
            localCounts.clear();
        }
    }

    // -----------------------------------------------------------------------
    // Reducer – standard sum
    // -----------------------------------------------------------------------
    /**
     * Aggregates the counts for each word pair to compute the final co-occurrence frequency.
     */
    public static class PairsSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    // -----------------------------------------------------------------------
    // Driver
    // -----------------------------------------------------------------------
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
            System.err.println("Usage: PairsClassLevelAggregation [-D cooccur.distance=<d>] " +
                    "-topwords <top50words.txt> <input> <output>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Pairs Class-Level Aggregation d=" +
                conf.getInt("cooccur.distance", 1));
        job.setJarByClass(PairsClassLevelAggregation.class);

        job.setMapperClass(PairsClassMapper.class);
        job.setReducerClass(PairsSumReducer.class);

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
        int res = ToolRunner.run(new Configuration(), new PairsClassLevelAggregation(), args);
        System.exit(res);
    }
}
