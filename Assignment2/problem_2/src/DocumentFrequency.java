import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import opennlp.tools.stemmer.PorterStemmer;

public class DocumentFrequency {

	private static void applyLocalExecutionDefaults(Configuration conf) {
		// Force conservative defaults for Hadoop local mode on a single machine.
		conf.setInt("mapreduce.local.map.tasks.maximum", 1);
		conf.setInt("mapreduce.task.io.sort.mb", 32);
		conf.set("mapreduce.map.java.opts", "-Xmx256m");
		conf.set("mapreduce.reduce.java.opts", "-Xmx256m");
	}

	public static class DocumentFrequencyMapper extends Mapper<Text, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		private static final Set<String> NOISE_TERMS = new HashSet<String>();
		static {
			NOISE_TERMS.add("apo");
			NOISE_TERMS.add("categori");
			NOISE_TERMS.add("extern");
			NOISE_TERMS.add("http");
			NOISE_TERMS.add("link");
			NOISE_TERMS.add("quot");
			NOISE_TERMS.add("refer");
		}

		private final Text outputKey = new Text();
		private final PorterStemmer stemmer = new PorterStemmer();
		private Set<String> stopwords = new HashSet<String>();
		private Set<String> stemmedStopwords = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();
			if (cacheFiles == null) {
				return;
			}

			for (URI cacheFile : cacheFiles) {
				Path cachePath = new Path(cacheFile.getPath());
				if ("stopwords.txt".equals(cachePath.getName())) {
					stopwords = TextProcessingUtils.loadWordSet(cachePath.getName());
					stemmedStopwords = new HashSet<String>();
					for (String stopword : stopwords) {
						String stemmedStopword = TextProcessingUtils.normalizeToken(stopword, stemmer);
						if (stemmedStopword != null) {
							stemmedStopwords.add(stemmedStopword);
						}
					}
				}
			}
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("[^\\p{L}\\p{Nd}']+");
			Set<String> uniqueTermsInDocument = new HashSet<String>();

			for (String token : tokens) {
				String rawNormalized = TextProcessingUtils.normalizeRawToken(token);
				if (rawNormalized == null || stopwords.contains(rawNormalized)) {
					continue;
				}

				String normalized = TextProcessingUtils.normalizeToken(rawNormalized, stemmer);
				if (normalized == null || stopwords.contains(normalized) || stemmedStopwords.contains(normalized)
						|| NOISE_TERMS.contains(normalized)) {
					continue;
				}
				uniqueTermsInDocument.add(normalized);
			}

			for (String term : uniqueTermsInDocument) {
				outputKey.set(term);
				context.write(outputKey, ONE);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: DocumentFrequency <input> <output> <stopwords-file>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		applyLocalExecutionDefaults(conf);
		System.out.println("Local config: map.tasks.max="
				+ conf.getInt("mapreduce.local.map.tasks.maximum", -1)
				+ ", sort.mb=" + conf.getInt("mapreduce.task.io.sort.mb", -1)
				+ ", map.opts=" + conf.get("mapreduce.map.java.opts")
				+ ", reduce.opts=" + conf.get("mapreduce.reduce.java.opts"));
		Job job = Job.getInstance(conf, "document-frequency");

		job.setJarByClass(DocumentFrequency.class);
		job.setMapperClass(DocumentFrequencyMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		job.addCacheFile(new Path(args[2]).toUri());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
