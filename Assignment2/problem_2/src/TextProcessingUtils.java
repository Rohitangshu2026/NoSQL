import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import opennlp.tools.stemmer.PorterStemmer;

public final class TextProcessingUtils {

	private TextProcessingUtils() {
	}

	public static Set<String> loadWordSet(String fileName) throws IOException {
		Set<String> words = new HashSet<String>();
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				String normalized = line.trim().toLowerCase(Locale.ROOT);
				if (!normalized.isEmpty()) {
					words.add(normalized);
				}
			}
		} finally {
			reader.close();
		}
		return words;
	}

	public static String normalizeToken(String token, PorterStemmer stemmer) {
		String normalized = normalizeRawToken(token);
		if (normalized == null) {
			return null;
		}

		String stemmed = stemmer.stem(normalized).toString().trim();
		return stemmed.isEmpty() ? null : stemmed;
	}

	public static String normalizeRawToken(String token) {
		if (token == null) {
			return null;
		}

		String normalized = token.toLowerCase(Locale.ROOT).replaceAll("(^'+|'+$)", "");
		if (normalized.isEmpty()) {
			return null;
		}

		boolean hasLetter = false;
		for (int i = 0; i < normalized.length(); i++) {
			char current = normalized.charAt(i);
			if (Character.isDigit(current)) {
				return null;
			}
			if (Character.isLetter(current)) {
				hasLetter = true;
			}
		}
		if (!hasLetter) {
			return null;
		}
		if (normalized.length() < 3) {
			return null;
		}

		return normalized;
	}
}
