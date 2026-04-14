import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
 * A MapReduce job to generate the co-occurring word matrix using the Stripes
 * approach
 * with class-level local aggregation.
 */
public class StripesClassLevelAggregation extends Configured implements Tool {

    /**
     * Maintains an in-memory aggregation buffer of stripes across the entire task
     * (class-level) before emitting them during the cleanup phase.
     */
    public static class StripesClassMapper extends Mapper<Object, Text, Text, MapWritable> {

        private int distance = 1;
        private Set<String> topWords = new HashSet<>();

        // Nested aggregation buffer: centerWord -> { neighbor -> count }
        // Lives for the entire mapper task lifetime (map-CLASS level)
        private Map<String, Map<String, Integer>> localAgg = new HashMap<>();

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
                if (token.isEmpty())
                    continue;
                if (token.length() < 2)
                    continue;
                validWords.add(token);
            }

            for (int i = 0; i < validWords.size(); i++) {
                String center = validWords.get(i);
                if (!topWords.contains(center))
                    continue;

                Map<String, Integer> stripe = localAgg.computeIfAbsent(center, k -> new HashMap<>());

                int start = Math.max(0, i - distance);
                int end = Math.min(validWords.size() - 1, i + distance);

                for (int j = start; j <= end; j++) {
                    if (j == i)
                        continue;
                    String neighbor = validWords.get(j);
                    if (!topWords.contains(neighbor))
                        continue;

                    stripe.merge(neighbor, 1, Integer::sum);
                }
            }
            // No context.write here — flushed in cleanup()
        }

        /**
         * Called ONCE after all map() calls for this mapper instance.
         * Converts local buffer to MapWritable and emits.
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Text outKey = new Text();
            for (Map.Entry<String, Map<String, Integer>> entry : localAgg.entrySet()) {
                outKey.set(entry.getKey());
                MapWritable stripe = new MapWritable();
                for (Map.Entry<String, Integer> e : entry.getValue().entrySet()) {
                    stripe.put(new Text(e.getKey()), new IntWritable(e.getValue()));
                }
                context.write(outKey, stripe);
            }
            localAgg.clear();
        }
    }

    /**
     * Merges all individual stripes for a given center word to compute the total
     * co-occurrence counts and emits the final aggregated stripe as a formatted
     * string.
     */
    public static class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<MapWritable> stripes, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> aggregated = new HashMap<>();
            for (MapWritable stripe : stripes) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    String neighbor = entry.getKey().toString();
                    int count = ((IntWritable) entry.getValue()).get();
                    aggregated.merge(neighbor, count, Integer::sum);
                }
            }

            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Integer> e : aggregated.entrySet()) {
                if (!first)
                    sb.append(", ");
                sb.append(e.getKey()).append(":").append(e.getValue());
                first = false;
            }
            sb.append("}");
            context.write(key, new Text(sb.toString()));
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
            System.err.println("Usage: StripesClassLevelAggregation [-D cooccur.distance=<d>] " +
                    "-topwords <top50words.txt> <input> <o>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Stripes Class-Level Aggregation d=" +
                conf.getInt("cooccur.distance", 1));
        job.setJarByClass(StripesClassLevelAggregation.class);

        job.setMapperClass(StripesClassMapper.class);
        job.setReducerClass(StripesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (topWordsFile != null)
            job.addCacheFile(new Path(topWordsFile).toUri());

        FileInputFormat.setMaxInputSplitSize(job, 33554432L);
        FileInputFormat.addInputPath(job, new Path(remainingArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs.get(1)));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new StripesClassLevelAggregation(), args);
        System.exit(res);
    }
}
