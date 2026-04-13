import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import opennlp.tools.stemmer.PorterStemmer;

public class VerifyTfIdf {

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

	private static final double EPS = 1e-6;

	private final PorterStemmer stemmer = new PorterStemmer();
	private Set<String> stopwords = new HashSet<String>();
	private Set<String> stemmedStopwords = new HashSet<String>();
	private Map<String, Integer> dfByTerm = new HashMap<String, Integer>();
	private Map<String, Double> outputScores = new HashMap<String, Double>();

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: VerifyTfIdf <dataset-dir> <top100_df.tsv> <tfidf_scores_full.tsv> <stopwords.txt>");
			System.exit(1);
		}

		VerifyTfIdf verifier = new VerifyTfIdf();
		verifier.loadStopwords(args[3]);
		verifier.loadDfFile(args[1]);
		verifier.loadOutputScores(args[2]);

		VerificationResult result = verifier.verifyDataset(args[0]);
		if (!result.success) {
			System.err.println("Verification failed.");
			System.err.println(result.summary);
			System.exit(2);
		}

		System.out.println("Verification succeeded.");
		System.out.println(result.summary);
	}

	private void loadStopwords(String stopwordsPath) throws IOException {
		stopwords = TextProcessingUtils.loadWordSet(stopwordsPath);
		stemmedStopwords = new HashSet<String>();
		for (String stopword : stopwords) {
			String stemmedStopword = TextProcessingUtils.normalizeToken(stopword, stemmer);
			if (stemmedStopword != null) {
				stemmedStopwords.add(stemmedStopword);
			}
		}
	}

	private void loadDfFile(String dfPath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(dfPath));
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

	private void loadOutputScores(String outputPath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(outputPath));
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("\\t");
				if (parts.length < 3) {
					continue;
				}
				String key = parts[0] + "\t" + parts[1];
				outputScores.put(key, Double.parseDouble(parts[2]));
			}
		} finally {
			reader.close();
		}
	}

	private VerificationResult verifyDataset(String datasetDir) throws IOException {
		File root = new File(datasetDir);
		if (!root.exists() || !root.isDirectory()) {
			return new VerificationResult(false, "Dataset directory not found: " + datasetDir);
		}

		int docsProcessed = 0;
		int mismatches = 0;
		int missing = 0;
		int matched = 0;
		StringBuilder firstErrors = new StringBuilder();

		File[] files = root.listFiles();
		if (files == null) {
			return new VerificationResult(false, "No files found in dataset directory: " + datasetDir);
		}

		for (File file : files) {
			if (!file.isFile()) {
				continue;
			}
			String docId = file.getName().replaceFirst("\\.txt$", "");
			Map<String, Integer> tfByTerm = computeTfForDoc(file);
			docsProcessed++;

			for (Map.Entry<String, Integer> entry : tfByTerm.entrySet()) {
				String term = entry.getKey();
				int tf = entry.getValue();
				int df = dfByTerm.get(term);
				double expected = tf * Math.log((10000.0 / df) + 1.0);
				String key = docId + "\t" + term;
				Double actual = outputScores.remove(key);
				if (actual == null) {
					missing++;
					if (firstErrors.length() < 1000) {
						firstErrors.append("Missing output: ").append(key).append("\n");
					}
					continue;
				}
				if (Math.abs(expected - actual) > EPS) {
					mismatches++;
					if (firstErrors.length() < 1000) {
						firstErrors.append("Mismatch: ").append(key)
								.append(" expected=").append(expected)
								.append(" actual=").append(actual).append("\n");
					}
				} else {
					matched++;
				}
			}
		}

		int extra = outputScores.size();
		StringBuilder summary = new StringBuilder();
		summary.append("Docs processed: ").append(docsProcessed).append("\n");
		summary.append("Matched: ").append(matched).append("\n");
		summary.append("Mismatched: ").append(mismatches).append("\n");
		summary.append("Missing: ").append(missing).append("\n");
		summary.append("Extra output lines: ").append(extra).append("\n");
		if (firstErrors.length() > 0) {
			summary.append("\nExamples:\n").append(firstErrors);
		}

		boolean success = mismatches == 0 && missing == 0 && extra == 0;
		return new VerificationResult(success, summary.toString());
	}

	private Map<String, Integer> computeTfForDoc(File file) throws IOException {
		Map<String, Integer> tfByTerm = new HashMap<String, Integer>();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				for (String token : tokenize(line)) {
					String rawNormalized = TextProcessingUtils.normalizeRawToken(token);
					if (rawNormalized == null || stopwords.contains(rawNormalized)) {
						continue;
					}

					String normalized = TextProcessingUtils.normalizeToken(rawNormalized, stemmer);
					if (normalized == null || stopwords.contains(normalized) || stemmedStopwords.contains(normalized)
							|| NOISE_TERMS.contains(normalized) || !dfByTerm.containsKey(normalized)) {
						continue;
					}

					Integer current = tfByTerm.get(normalized);
					if (current == null) {
						tfByTerm.put(normalized, 1);
					} else {
						tfByTerm.put(normalized, current + 1);
					}
				}
			}
		} finally {
			reader.close();
		}
		return tfByTerm;
	}

	private static String[] tokenize(String text) {
		StringBuilder current = new StringBuilder();
		java.util.List<String> tokens = new java.util.ArrayList<String>();
		for (int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);
			if (Character.isLetter(c) || Character.isDigit(c) || c == '\'') {
				current.append(c);
			} else if (current.length() > 0) {
				tokens.add(current.toString());
				current.setLength(0);
			}
		}
		if (current.length() > 0) {
			tokens.add(current.toString());
		}
		return tokens.toArray(new String[0]);
	}

	private static class VerificationResult {
		private final boolean success;
		private final String summary;

		private VerificationResult(boolean success, String summary) {
			this.success = success;
			this.summary = summary;
		}
	}
}
