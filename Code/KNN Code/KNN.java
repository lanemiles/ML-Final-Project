
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.lang.ProcessBuilder;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

/**
 * Implementation of a KNN classifier using hadoop.
 */
public class KNN {
	// Map from word to feature index.
	private Map<String, Integer> vocab; 
	// Name of the directory for the input to the hadoop processes.
	private long inputFileDir;
	private int k = 25;

	// Counters to track how many examples have been classified and how many
	// have been classified correctly across all threads.
	private AtomicInteger seenSoFar = new AtomicInteger();
	private AtomicInteger numCorrect = new AtomicInteger();

	public static void main(String[] args) throws IOException, InterruptedException {

		shuffleAndPartitionFile("documents_1k.train.txt", "test.txt", "train.txt", 0.00007);
		KNN knn = new KNN();				
		knn.setNumThreads(num_t);
		knn.train("train.txt");
		knn.classify("test.txt");
		knn.cleanUp();

		}
		

	}


	/**
	 * Takes a file of <label><tab><text> and converts it into a frequency map
	 * of words in the text, outputting a file of <label><tab><featureMap>
	 */
	public void train(String inputFile) {
		String outputFile = "vectorized_data.txt";

		vocab = new HashMap<String, Integer>();
		HashMap<Integer, Integer> perLineCounter = new HashMap<Integer, Integer>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(inputFile));
			PrintWriter writer = new PrintWriter(outputFile);
			for (String line = in.readLine(); line != null; line = in.readLine()) {
				// Split label and text.
				String[] tokens = line.split("\t");
				if (tokens.length >= 2) {
					// Split the text into words, convert to a frequency map,
					// and write label and map representation of text to output
					// file.
					String[] words = tokens[1].split(" ");
					processTextToMap(words, perLineCounter);
					writer.println(tokens[0] + "\t" + perLineCounter);
				}
			}
			in.close();
			writer.close();
		} catch (FileNotFoundException e) {
			System.out.println(
					"Error opening file containing training examples or opening output file.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println(
					"Error reading or closing file containing training examples, or closing output file.");
			e.printStackTrace();
		}

		try {
			// Make a input directory for hadoop processes.
			inputFileDir = System.currentTimeMillis();
			String command = String.format("hdfs dfs -mkdir /%d", inputFileDir);
			// String command = String.format("hdfs dfs -mkdir /%d", inputFileDir);
			Process p = Runtime.getRuntime().exec(command);
			p.waitFor();

			// Move the processed training data files to the hadoop directory.

			command = String.format("hdfs dfs -put %s /%d", outputFile, inputFileDir);
			// command = String.format("hadoop fs -D fs.azure.block.size=134217728 -put %s /%d", outputFile, inputFileDir);
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
		} catch (IOException e) {
			System.out.println("Error starting a training process.");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("Error waiting for training processes to finish.");
			e.printStackTrace();
		}
	}

	/**
	 * Delete auto-named directories from local and hadoop file systems.
	 */
	public void cleanUp() {
		try {
			// Clean local file system.
			ProcessBuilder pbuilder = new ProcessBuilder("/bin/bash", "-c", "rm -rf 148*");
			Process p = pbuilder.start();
			p.waitFor();

			// Clean hadoop file system.
			pbuilder = new ProcessBuilder("hdfs", "dfs", "-rm", "-r", "/148*");
			p = pbuilder.start();
			p.waitFor();

			// System.out.println("Time taken ( " + this.numTestExs + " exs) : " + (System.currentTimeMillis() - this.startTime));

		} catch (IOException e) {
			System.out.println("Error starting a cleanup process.");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("Error waiting for cleanup process to finish.");
			e.printStackTrace();
		}
	}

	/**
	 * A methods that takes the name of a file containing test examples
	 * represented as <label><tab><text> and classifies them by starting hadoop
	 * processes for each example, printing the accuracy at the end of the
	 * process.
	 */
	public void classify(String classifyFile) {

		try {
			BufferedReader in = new BufferedReader(new FileReader(classifyFile));

			HashMap<Integer, Integer> perLineCounter = new HashMap<Integer, Integer>();
			ArrayList<String> testExamples = new ArrayList<String>();
			ArrayList<Integer> testLabels = new ArrayList<Integer>();

			for (String line = in.readLine(); line != null; line = in.readLine()) {
				// Split label and text.
				String[] tokens = line.split("\t");
				// If the line is not blank and contains text:
				if (tokens.length >= 2) {
					testLabels.add(Integer.parseInt(tokens[0]));

					// Split text into words and create frequency map indexed by
					// feature corresponding to word.
					String[] words = tokens[1].split(" ");
					processTextToMap(words, perLineCounter);

					// Save the string corresponding to this map.
					testExamples.add(perLineCounter.toString());
				}
			}
			in.close();

			// Create and start threads on partitions of the test examples.
			ArrayList<KNNThread> threads = new ArrayList<KNNThread>();

			// update num test exs
			this.numTestExs = testExamples.size();
			int numThreads = 16;
			for (int i = 0; i < numThreads; i++) {
				int startIndex = (int) ((i * 1.0 / numThreads) * testExamples.size());
				int endIndex = (int) (((i + 1*1.0) / numThreads) * testExamples.size());
				// System.out.println(startIndex + " | " + endIndex);
				KNNThread newThread = new KNNThread(testExamples.subList(startIndex, endIndex),
						testLabels.subList(startIndex, endIndex));
				threads.add(newThread);
			}

			
			for (KNNThread thread : threads) {
				thread.start();
			}

			// Wait for the threads to finish.
			for (KNNThread thread : threads) {
				thread.join();
			}

			// Write the accuracy to a file and print it to the terminal.
			// System.out.println("Accuracy: " + numCorrect.get()*1.0 / testExamples.size());

		} catch (FileNotFoundException e) {
			System.out
					.println("Error opening file containing test examples or opening output file.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error reading file containing test examples.");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("Error waiting for threads to finish.");
			e.printStackTrace();
		}

	}

	/**
	 * A private method to translate an array of words into a frequency map,
	 * counting how many times each word occurs in the array. The map is indexed
	 * by the word's feature number, stored in the vocabulary. We add words to
	 * the vocabulary if we have not yet seen them.
	 */
	private void processTextToMap(String[] words, Map<Integer, Integer> perLineCounter) {
		perLineCounter.clear();
		for (String word : words) {

			// Update the vocabulary if necessary and get the feature index for
			// word.
			if (!vocab.containsKey(word)) {
				vocab.put(word, vocab.size());
			}
			int index = vocab.get(word);

			// Update the frequency map.
			if (perLineCounter.containsKey(index)) {
				perLineCounter.put(index, perLineCounter.get(index) + 1);
			} else {
				perLineCounter.put(index, 1);
			}
		}
	}

	/**
	 * A thread class that processes the subset of the test examples passed to
	 * it, updating the global count of examples classified correctly and
	 * examples seen so far.
	 */
	public class KNNThread extends Thread {

		private List<String> testExamples;
		private List<Integer> testLabels;

		/**
		 * @param testExamples
		 *            - the test examples to process.
		 * @param testLabels
		 *            - the labels that correspond to those test examples.
		 */
		public KNNThread(List<String> testExamples, List<Integer> testLabels) {
			this.testExamples = testExamples;
			this.testLabels = testLabels;
		}

		/**
		 * This method processes the test examples, classifying them with a
		 * hadoop process and determining if that classification is correct.
		 */
		@Override
		public void run() {

			// System.out.println("New thread - " + this.getId());
			// System.out.println(this.testExamples.get(0));

			String testEx;
			for (int x = 0; x < testExamples.size(); x++) {
				testEx = testExamples.get(x);

				// Remove spaces from the textEx so that we can use it as a
				// command line argument.
				String escapedStr = testEx.replace(" ", "_");
				// Use the current time and the thread id in order to create a
				// unique output directory.
				String outputDir = Long.toString(System.currentTimeMillis())
						+ Long.toString(this.getId());
				// The hadoop command we need to run:
				ProcessBuilder pbuilder = new ProcessBuilder("yarn", "jar",
						"/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar", "-files",
						"knn_mapper.py,knn_reducer.py", "-mapper", "knn_mapper.py", "-reducer",
						"knn_reducer.py", "-input", "wasbs:///" + inputFileDir, "-output",
						"wasbs:///" + outputDir, "-cmdenv", "EXAMPLE_STR=" + escapedStr);
				try {
					// Call the Hadoop command and what for it to finish.
					Process p = pbuilder.start();
					// System.out.println("Started new Hadoop job");
					// System.out.println("Thread " + this.getId() + " is now waiting for HP");
					p.waitFor();
					// System.out.println("Thread " + this.getId() + " is done with HP!");
					int zeroCount = 0;

					// Get the output directory from hadoop.
					String get = "hdfs dfs -get /" + outputDir;
					// System.out.println("Thread " + this.getId() + " is now waiting for FILE");
					p = Runtime.getRuntime().exec(get);
					p.waitFor();
					// System.out.println("Thread " + this.getId() + " is done with FILE!");
					


					// Read the labels of the k most similar examples. Every
					// other line is blank, so we consider the first 2k lines of
					// the file.
					BufferedReader in = new BufferedReader(
							new FileReader(outputDir + "/part-00000"));
					for (int i = 0; i < 2 * k; i++) {
						String[] pieces = in.readLine().split("\t");
						if (pieces.length > 1 && pieces[1].equals("0")) {
							zeroCount += 1;
						}
					}
					in.close();

					// Determine the predication based on the number of zero
					// labels we saw. Update numCorrect if the prediction is
					// correct.
					int prediction = zeroCount <= k / 2 ? 1 : 0;
					if (prediction == testLabels.get(x)) {
						numCorrect.getAndIncrement();
					}
					// System.out.println("Processed example: " + seenSoFar.getAndIncrement() + " | Prediction is " + prediction + " | Label is " + testLabels.get(x));

				} catch (FileNotFoundException e) {
					System.out.println("Couldn't find the output file from the hadoop process.");
					e.printStackTrace();
				} catch (InterruptedException e) {
					System.out.println("Error waiting for hadoop process or waiting for process of "
							+ "getting hadoop output directory.");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println(
							"Error reading from the output file or starting one of the processes.");
				}
			}
		}
	}

	/**
	 * A method that reads the input file containing the training data. This
	 * method assumes that the file contains aligned data for a binary
	 * classification problem such that the aligned examples are interleaved one
	 * after the other, and keeps these aligned examples together, either both
	 * in the test file or both in the train file. If the data does not fit this
	 * format, that does not cause any problems, but pairs are still kept
	 * together and if there are an odd number of examples the last one is
	 * thrown away.
	 */
	public static void shuffleAndPartitionFile(String inputFileName, String test, String train,
			double percentageTest) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(inputFileName));
			List<Pair> lines = new ArrayList<Pair>();
			for (String one = in.readLine(); one != null; one = in.readLine()) {
				String two = in.readLine();
				if (two == null) {
					break;
				}
				lines.add(new Pair(one, two));
			}
			in.close();
			Collections.shuffle(lines);
			PrintWriter trainOut = new PrintWriter(train);
			PrintWriter testOut = new PrintWriter(test);
			int numTest = (int) (percentageTest * lines.size());
			for (int i = 0; i < numTest; i++) {
				testOut.println(lines.get(i).one);
				testOut.println(lines.get(i).two);
			}
			testOut.close();
			for (int i = numTest; i < lines.size(); i++) {
				trainOut.println(lines.get(i).one);
				trainOut.println(lines.get(i).two);
			}
			trainOut.close();
		} catch (FileNotFoundException e) {
			System.out.println("Input training data file was not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error shuffling and partitioning training data.");
			e.printStackTrace();
		}
	}

	/**
	 * A helper class for partitioning into training and test datasets while
	 * keeping aligned pairs together.
	 */
	public static class Pair {
		String one;
		String two;

		public Pair(String one, String two) {
			this.one = one;
			this.two = two;
		}
	}

}
