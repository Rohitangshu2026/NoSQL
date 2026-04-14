import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

/**
 * An advanced MapReduce job to count word frequencies, with support for
 * filtering
 * out a custom list of stop words and managing case sensitivity.
 */
public class WordCount2 extends Configured implements Tool {

    /**
     * Tokenizes input lines, applies case sensitivity preferences, filters out
     * specified stop words and short tokens, and emits valid words with a count of
     * one.
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive = false;
        private Set<String> patternsToSkip = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = reader.readLine()) != null) {
                    patternsToSkip.add(pattern.toLowerCase().trim());
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println(
                        "Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            line = line.replaceAll("&[a-z]+;", "");
            line = line.replaceAll("<[^>]+>", "");
            line = line.replaceAll("(https?://|www\\.)\\S+", "");
            line = line.replaceAll("\\[\\[[^\\]]+\\]\\]", "");
            String[] tokens = line.split("[^a-zA-Z]+");
            for (String token : tokens) {
                if (token.isEmpty())
                    continue;
                if (token.length() < 2)
                    continue;
                if (patternsToSkip.contains(token))
                    continue;
                word.set(token);
                context.write(word, one);
            }
        }
    }

    /**
     * Aggregates the local and global counts for each word to produce the final
     * frequencies.
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

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
        Job job = Job.getInstance(conf, "wordcount2");
        job.setJarByClass(WordCount2.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int argIndex = 0;
        for (int i = 0; i < args.length; ++i) {
            if ("-skippatterns".equals(args[i])) {
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                job.addCacheFile(new Path(args[++i]).toUri());
            } else if ("-casesensitive".equals(args[i])) {
                job.getConfiguration().setBoolean("wordcount.case.sensitive", true);
            } else {
                if (argIndex == 0) {
                    FileInputFormat.addInputPath(job, new Path(args[i]));
                    argIndex++;
                } else if (argIndex == 1) {
                    FileOutputFormat.setOutputPath(job, new Path(args[i]));
                    argIndex++;
                }
            }
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount2(), args);
        System.exit(res);
    }
}