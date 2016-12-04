import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.lang.ProcessBuilder;


public class KNN {

	private Map<String, Integer> vocab;
	private long inputFileDir;
	private String outputFileName;

	public static void main(String[] args) throws IOException, InterruptedException {

		
		KNN knn = new KNN();
		knn.train("baby.txt", "vectorized_data.txt");
		knn.classify("vectorized_data.txt");
	}

  	// Takes a file of <label><tab><text> and converts it into
    // map of features and values (by counting words) of
    // <label><tab><featureMap>
	public void train(String inputFile, String outputFile)
			throws IOException, InterruptedException {
		outputFileName = outputFile;
		vocab = new HashMap<String, Integer>();
		HashMap<Integer, Integer> perLineCounter = new HashMap<Integer, Integer>();

		// open the file
		BufferedReader in = new BufferedReader(new FileReader(inputFile));
		PrintWriter writer = new PrintWriter(outputFile);
		String line = in.readLine();
		int index;
		for (; line != null; line = in.readLine()) {
			perLineCounter.clear();

			// split label and sentence
			String[] tokens = line.split("\t");

			if (tokens.length >= 2) {
				// split sentence
				String[] words = tokens[1].split(" ");

				for (String word : words) {
					// create map of word to index
					if (!vocab.containsKey(word)) {
						vocab.put(word, vocab.size());
					}
					index = vocab.get(word);

					// create map of <word_index, count>
					if (perLineCounter.containsKey(index)) {
						perLineCounter.put(index, perLineCounter.get(index) + 1);
					} else {
						perLineCounter.put(index, 1);
					}

				}
				// write this line's map to file
				writer.println(tokens[0] + "\t" + perLineCounter);
			}
		}

		in.close();
		writer.close();

		inputFileDir = System.currentTimeMillis();
		System.out.println("the input file directory is: " + inputFileDir);
//		String command1 = String.format("mkdir %d", inputFileDir);
//		String command2 = String.format("mv %s %d", outputFile, inputFileDir);
		String command1 = String.format("hdfs dfs -mkdir /%d", inputFileDir);
		String command2 = String.format("hdfs dfs -put %s /%d", outputFile, inputFileDir);
		Process p = Runtime.getRuntime().exec(command1);
		p.waitFor();
		p = Runtime.getRuntime().exec(command2);
		p.waitFor();
	}

	
	
	public void cleanUp() throws InterruptedException, IOException {
		String commands = String.format("rm %s && hdfs dfs -rm %d/* && hdfs dfs -rmdir %d",
				outputFileName, inputFileDir, inputFileDir);
		Process p = Runtime.getRuntime().exec(commands);
		p.waitFor();
	}

	public void classify(String classifyFile) throws IOException, FileNotFoundException, InterruptedException {

		BufferedReader in = new BufferedReader(new FileReader(classifyFile));
		PrintWriter writer = new PrintWriter("output.txt");
		String line = in.readLine();
		int index;

		HashMap<Integer, Integer> perLineCounter = new HashMap<Integer, Integer>();
		ArrayList<String> testExamples = new ArrayList<String>();

		for (; line != null; line = in.readLine()) {
			perLineCounter.clear();

			// split label and sentence
			String[] tokens = line.split("\t");

			if (tokens.length >= 2) {
				// split sentence
				String[] words = tokens[1].split(" ");

				for (String word : words) {
					// create map of word to index
					if (!vocab.containsKey(word)) {
						vocab.put(word, vocab.size());
					}
					index = vocab.get(word);

					// create map of <word_index, count>
					if (perLineCounter.containsKey(index)) {
						perLineCounter.put(index, perLineCounter.get(index) + 1);
					} else {
						perLineCounter.put(index, 1);
					}

				}
				// write this line's map to file
				testExamples.add(perLineCounter.toString());
			}
		}

		in.close();

		// iterate over test exmaples, classifying each one
		for (String testEx : testExamples) {

			int thisLabel = 0;

			// escape string and build up hadoop command
			String escapedStr = testEx.replace(" ", "_");
			long outputDir = System.currentTimeMillis();
			ProcessBuilder pbuilder = new ProcessBuilder("yarn", "jar", 
				"/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar", 
				"-files", "knn_mapper.py,knn_reducer.py", "-mapper", "knn_mapper.py", 
				"-reducer", "knn_reducer.py", "-input", "wasbs:///"+Long.toString(inputFileDir),
				"-output", "wasbs:///"+Long.toString(outputDir), "-cmdenv", "EXAMPLE_STR="+escapedStr);
			System.out.println("PBUILDER IS: " + pbuilder.command());

			Process p = pbuilder.start();
			p.waitFor();
			int zero_count = 0;

			// read the output files and get top k
			String get = "hdfs dfs -get /" + outputDir;
			System.out.println(get);
			p = Runtime.getRuntime().exec(get);
			p.waitFor();
			in = new BufferedReader(new FileReader(outputDir + "/part-00000"));
			for (int i = 0; i < 5; i++) {
				line = in.readLine();
				System.out.println("line is " + line);
				String[] pieces = line.split("\t");
				System.out.println("pieces is " + pieces);
				if (pieces.length > 0 && pieces[1] == "0") {
					zero_count += 1;
				}
			}

			if (zero_count < 2) {
				thisLabel = 1;
			}

			writer.println(thisLabel);

		}

		writer.close();

	}
}
