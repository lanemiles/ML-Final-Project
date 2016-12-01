package preprocessing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

public class RemoveWordsAddLabels {
	
	public static void main(String[] args) throws IOException {
		Set<String> vocabWords = new HashSet<String>();
		
		BufferedReader in = new BufferedReader(new FileReader("sorted_total_wc.txt"));		
		String line = in.readLine();
		for (int i = 0; i < 50 && line != null; i++) {
			line = in.readLine();
		}
		
		for (int i = 0; i < 5000 && line != null; i++) {
			String[] wordAndCount = line.split("\t");
			vocabWords.add(wordAndCount[0]);
			line = in.readLine();
		}
		in.close();
			
		PrintWriter writer = new PrintWriter("sentences.train.txt", "UTF-8");
		writeProcessedDataWithLabel("lower_normal_no_tab.txt", 0, writer, vocabWords);
		writeProcessedDataWithLabel("lower_simple_no_tab.txt", 1, writer, vocabWords);
		writer.close();
	}
	
	private static void writeProcessedDataWithLabel(String filename, int label, PrintWriter writer,
			Set<String> vocabWords) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(filename));
		String line = in.readLine();
		while (line != null) {
			String[] words = line.split(" ");
			StringBuffer processedLine = new StringBuffer();
			for (String word : words) {
				// remove numbers as well?
				if (vocabWords.contains(word)) {
					processedLine.append(word + " ");
				}
			}
			writer.println(label + "\t" + processedLine.toString());
			line = in.readLine();
		}
		in.close();
	}
}