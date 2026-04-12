import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import opennlp.tools.stemmer.PorterStemmer;

public class TfIdfScore {

	private static void applyLocalExecutionDefaults(Configuration conf) {
		// Force conservative defaults for Hadoop local mode on a single machine.
		conf.setInt("mapreduce.local.map.tasks.maximum", 1);
		conf.setInt("mapreduce.task.io.sort.mb", 32);
		conf.set("mapreduce.map.java.opts", "-Xmx256m");
		conf.set("mapreduce.reduce.java.opts", "-Xmx256m");
	}

	public static class TfIdfMapper extends Mapper<Text, Text, Text, DoubleWritable> {

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

		private final PorterStemmer stemmer = new PorterStemmer();
		private final Text outputKey = new Text();
		private final DoubleWritable outputValue = new DoubleWritable();
		private Set<String> stopwords = new HashSet<String>();
		private Set<String> stemmedStopwords = new HashSet<String>();
		private Map<String, Integer> dfByTerm = new HashMap<String, Integer>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();
			if (cacheFiles == null) {
				return;
			}

			for (URI cacheFile : cacheFiles) {
				Path cachePath = new Path(cacheFile.getPath());
				String fileName = cachePath.getName();
				if ("stopwords.txt".equals(fileName)) {
					stopwords = TextProcessingUtils.loadWordSet(fileName);
					stemmedStopwords = new HashSet<String>();
					for (String stopword : stopwords) {
						String stemmedStopword = TextProcessingUtils.normalizeToken(stopword, stemmer);
						if (stemmedStopword != null) {
							stemmedStopwords.add(stemmedStopword);
						}
					}
				} else if ("top100_df.tsv".equals(fileName)) {
					loadDfFile(fileName);
				}
			}
		}

		private void loadDfFile(String fileName) throws IOException {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			try {
				String line;
				while ((line = reader.readLine()) != null) {
					String[] parts = line.trim().split("\\s+");
					if (parts.length < 2) {
						continue;
					}
					dfByTerm.put(parts[0], Integer.parseInt(parts[1]));
				}
			} finally {
				reader.close();
			}
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String documentId = key.toString().replaceFirst("\\.txt$", "");
			String[] tokens = value.toString().split("[^\\p{L}\\p{Nd}']+");
			Map<String, Integer> tfByTerm = new HashMap<String, Integer>();

			for (String token : tokens) {
				String rawNormalized = TextProcessingUtils.normalizeRawToken(token);
				if (rawNormalized == null || stopwords.contains(rawNormalized)) {
					continue;
				}

				String normalized = TextProcessingUtils.normalizeToken(rawNormalized, stemmer);
				if (normalized == null || stopwords.contains(normalized) || stemmedStopwords.contains(normalized)
						|| NOISE_TERMS.contains(normalized) || !dfByTerm.containsKey(normalized)) {
					continue;
				}

				Integer currentCount = tfByTerm.get(normalized);
				if (currentCount == null) {
					tfByTerm.put(normalized, 1);
				} else {
					tfByTerm.put(normalized, currentCount + 1);
				}
			}

			for (Map.Entry<String, Integer> entry : tfByTerm.entrySet()) {
				String term = entry.getKey();
				int tf = entry.getValue();
				int df = dfByTerm.get(term);
				double score = tf * Math.log((10000.0 / df) + 1.0);

				outputKey.set(documentId + "\t" + term);
				outputValue.set(score);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class ScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			for (DoubleWritable value : values) {
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: TfIdfScore <input> <output> <stopwords-file> <top100-df-file>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		applyLocalExecutionDefaults(conf);
		System.out.println("Local config: map.tasks.max="
				+ conf.getInt("mapreduce.local.map.tasks.maximum", -1)
				+ ", sort.mb=" + conf.getInt("mapreduce.task.io.sort.mb", -1)
				+ ", map.opts=" + conf.get("mapreduce.map.java.opts")
				+ ", reduce.opts=" + conf.get("mapreduce.reduce.java.opts"));
		Job job = Job.getInstance(conf, "tf-idf-score");

		job.setJarByClass(TfIdfScore.class);
		job.setMapperClass(TfIdfMapper.class);
		job.setReducerClass(ScoreReducer.class);

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(1);

		job.addCacheFile(new Path(args[2]).toUri());
		job.addCacheFile(new Path(args[3]).toUri());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
