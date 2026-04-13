import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopFrequentWords {

    public static class TopMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable count = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\t");
            if (parts.length != 2) return;

            try {
                word.set(parts[0]);
                count.set(Integer.parseInt(parts[1].trim()));
                context.write(word, count);
            } catch (NumberFormatException e) {
                // skip malformed lines
            }
        }
    }

    public static class TopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private PriorityQueue<Pair> minHeap;

        @Override
        protected void setup(Context context) {
            minHeap = new PriorityQueue<>();
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            minHeap.add(new Pair(key.toString(), sum));
            if (minHeap.size() > 50) minHeap.poll();
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            Pair[] arr = new Pair[minHeap.size()];
            int idx = 0;
            while (!minHeap.isEmpty()) arr[idx++] = minHeap.poll();

            for (int i = idx - 1; i >= 0; i--) {
                context.write(new Text(arr[i].word), new IntWritable(arr[i].count));
            }
        }
    }

    static class Pair implements Comparable<Pair> {
        String word;
        int count;

        Pair(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public int compareTo(Pair other) {
            return this.count - other.count;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopFrequentWords <wordcount-output> <top50-output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 50 Words");

        job.setJarByClass(TopFrequentWords.class);
        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}