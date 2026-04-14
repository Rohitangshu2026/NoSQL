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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A MapReduce job to generate the co-occurring word matrix using the Stripes
 * approach
 * without any explicit local aggregation.
 */
public class StripesCoOccurrence extends Configured implements Tool {

    /**
     * Emits a stripe (custom map) of neighbor counts for each center word found in
     * the input.
     */
    public static class StripesMapper extends Mapper<Object, Text, Text, MapWritable> {

        private int distance = 1;
        private Set<String> topWords = new HashSet<>();
        private Text centerWord = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            distance = conf.getInt("cooccur.distance", 1);

            // Load top-50 frequent words from distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    Path p = new Path(uri.getPath());
                    String fileName = p.getName();
                    if (fileName.equals("top50words.txt")) {
                        loadTopWords(fileName);
                    }
                }
            }
        }

        private void loadTopWords(String fileName) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // File produced by Problem 1a: "word <tab> count"
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 1) {
                        topWords.add(parts[0].toLowerCase());
                    }
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
            String[] tokens = line.split("[^a-zA-Z]+");

            // Build dense list of ALL valid tokens (not just top-50)
            java.util.List<String> validWords = new java.util.ArrayList<>();
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

                MapWritable stripe = new MapWritable();

                int start = Math.max(0, i - distance);
                int end = Math.min(validWords.size() - 1, i + distance);

                for (int j = start; j <= end; j++) {
                    if (j == i)
                        continue;
                    String neighbor = validWords.get(j);
                    if (!topWords.contains(neighbor))
                        continue;

                    Text neighborKey = new Text(neighbor);
                    if (stripe.containsKey(neighborKey)) {
                        IntWritable count = (IntWritable) stripe.get(neighborKey);
                        count.set(count.get() + 1);
                    } else {
                        stripe.put(neighborKey, new IntWritable(1));
                    }
                }

                if (!stripe.isEmpty()) {
                    centerWord.set(center);
                    context.write(centerWord, stripe);
                }
            }
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

            // Emit as human-readable string: "word\t{neighbor:count, ...}"
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

        // Parse custom arguments
        java.util.List<String> remainingArgs = new java.util.ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("-topwords".equals(args[i])) {
                topWordsFile = args[++i];
            } else {
                remainingArgs.add(args[i]);
            }
        }

        if (remainingArgs.size() < 2) {
            System.err.println("Usage: StripesCoOccurrence [-D cooccur.distance=<d>] " +
                    "-topwords <top50words.txt> <input> <output>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Stripes Co-Occurrence d=" +
                conf.getInt("cooccur.distance", 1));
        job.setJarByClass(StripesCoOccurrence.class);

        job.setMapperClass(StripesMapper.class);
        job.setReducerClass(StripesReducer.class);

        // Map output types differ from job output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (topWordsFile != null) {
            job.addCacheFile(new Path(topWordsFile).toUri());
        }

        FileInputFormat.setMaxInputSplitSize(job, 33554432L);
        FileInputFormat.addInputPath(job, new Path(remainingArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs.get(1)));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new StripesCoOccurrence(), args);
        System.exit(res);
    }
}
