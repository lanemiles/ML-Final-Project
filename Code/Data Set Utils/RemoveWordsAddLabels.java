import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This class reads in lines of text corresponding to examples, filters out
 * words are part of our stop list (the top NUM_STOP_WORDS most frequent words
 * in the text) and words that are not in our vocabulary (the top VOCAB_SIZE
 * most frequent words that follow the stop list - setting this to
 * Integer.MAX_VALUE means that every word not in our stop list will be in the
 * vocabulary). It then writes <label><tab><filtered text> to another file where
 * the label depends on whether we read the example from the simple or normal
 * english source file. This is a *one-time* pre-processing step given the data,
 * vocabulary size, and stop list size we want to use.
 * 
 * We pair aligned simple and normal document/sentences so that we can later
 * ensure that the corresponding simple and normal documents/sentences are both
 * in train or both in test, in the cases that there is an even alignment of the
 * data.
 */
public class RemoveWordsAddLabels {

	public static final int NUM_STOP_WORDS = 50;
	public static final long VOCAB_SIZE = Long.MAX_VALUE;
	public static final String OUTPUT_FILE_NAME = "sentences.train.txt";
	public static final String WORD_COUNT_FILE = "sorted_total_wc.txt";
	public static final String SIMPLE_FILE_JUST_TEXT = "lower_simple_no_tab.txt";
	public static final String NORMAL_FILE_JUST_TEXT = "lower_normal_no_tab.txt";

	public static final Pattern PATTERN = Pattern.compile("[0-9]+");
	public static final CharsetEncoder ASCII_ENCODER = Charset.forName("US-ASCII").newEncoder();

	/**
	 * This method drives the conversion of two files containing lines of text,
	 * to a file formatted as <label><tab><filtered text>, as described in
	 * detail above.
	 */
	public static void main(String[] args) {
		Set<String> vocabWords = new HashSet<String>();

		try {
			BufferedReader in = new BufferedReader(new FileReader(WORD_COUNT_FILE));
			String line = in.readLine();
			// Ignore stopwords.
			for (int i = 0; i < NUM_STOP_WORDS && line != null; i++) {
				line = in.readLine();
			}
			// Read in the vocabulary.
			for (int i = 0; i < VOCAB_SIZE && line != null; i++) {
				String[] wordAndCount = line.split("\t");
				vocabWords.add(wordAndCount[0]);
				line = in.readLine();
			}
			in.close();
		} catch (FileNotFoundException e) {
			System.out.println("Error opening word count file " + WORD_COUNT_FILE);
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error closing or reading in vocabulary from " + WORD_COUNT_FILE);
			e.printStackTrace();
		}

		// Write processed data to output file.
		try {
			writeProcessedDataWithLabel(SIMPLE_FILE_JUST_TEXT, NORMAL_FILE_JUST_TEXT, vocabWords);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			System.out.println("Error opening one of " + OUTPUT_FILE_NAME + ", "
					+ SIMPLE_FILE_JUST_TEXT + ", or " + NORMAL_FILE_JUST_TEXT);
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error closing or writing to " + OUTPUT_FILE_NAME
					+ " or closing or reading from " + SIMPLE_FILE_JUST_TEXT + " or "
					+ NORMAL_FILE_JUST_TEXT);
			e.printStackTrace();
		}

	}

	/**
	 * Reads text from the files containing aligned simple and normal english
	 * wikipedia examples simultaneously, so as to output aligned examples one
	 * after the other.
	 * 
	 * @throws IOException, FileNotFoundException
	 *             - if thrown, these are caught in main.
	 */
	private static void writeProcessedDataWithLabel(String simpleFilename, String normalFilename,
			Set<String> vocabWords) throws IOException, FileNotFoundException {
		PrintWriter writer = new PrintWriter(OUTPUT_FILE_NAME, "UTF-8");
		BufferedReader simpleIn = new BufferedReader(new FileReader(simpleFilename));
		BufferedReader normalIn = new BufferedReader(new FileReader(normalFilename));
		String simpleLine = simpleIn.readLine();
		String normalLine = normalIn.readLine();
		while (simpleLine != null || normalLine != null) {
			if (simpleLine != null) {
				processLine(simpleLine, writer, vocabWords, 1);
				simpleLine = simpleIn.readLine();
			}
			if (normalLine != null) {
				processLine(normalLine, writer, vocabWords, 0);
				normalLine = normalIn.readLine();
			}
		}
		simpleIn.close();
		normalIn.close();
		writer.close();
	}

	/**
	 * Processes a line of text and writes <label><tab><filtered text> out via
	 * the writer passed in. Numbers in the text are replaced with "<num>" and
	 * words containing non-ascii character or that are not in the vocabulary
	 * are filtered out.
	 */
	private static void processLine(String line, PrintWriter writer, Set<String> vocabWords,
			int label) {
		String[] words = line.split(" ");
		StringBuffer processedLine = new StringBuffer();
		for (String word : words) {
			if (PATTERN.matcher(word).matches()) {
				processedLine.append("<num> ");
			} else if (vocabWords.contains(word) && ASCII_ENCODER.canEncode(word)) {
				processedLine.append(word + " ");
			}
		}
		writer.println(label + "\t" + processedLine.toString());
	}
}