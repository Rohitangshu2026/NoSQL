import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<Text, Text> {

	private FileSplit fileSplit;
	private Configuration configuration;
	private boolean processed = false;
	private Text currentKey = new Text();
	private Text currentValue = new Text();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.configuration = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (processed) {
			return false;
		}

		byte[] contents = new byte[(int) fileSplit.getLength()];
		Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(configuration);
		FSDataInputStream inputStream = null;

		try {
			inputStream = fs.open(file);
			IOUtils.readFully(inputStream, contents, 0, contents.length);
			currentKey.set(file.getName());
			currentValue.set(contents);
		} finally {
			IOUtils.closeStream(inputStream);
		}

		processed = true;
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
	}
}
