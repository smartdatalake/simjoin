package eu.smartdatalake.simjoin.runners;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollectionReader;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetSimJoin;
import eu.smartdatalake.simjoin.MatchingPair;

/**
 * Executes fuzzy set similarity join operations according to the provided
 * configuration parameters.
 *
 */
public class RunFuzzySetSimJoin {

	public void execute(JSONObject config) {
		try {
			/* READ PARAMETERS */
			// operation
			String joinType = String.valueOf(config.get("join_type"));

			// input & output
			String inputFile = String.valueOf(config.get("input_file"));
			String queryFile = String.valueOf(config.get("query_file"));
			if (queryFile.equals("null") || queryFile.equals(""))
				queryFile = null;
			int maxLines = Integer.parseInt(String.valueOf(config.get("max_lines")));
			String outputFile = String.valueOf(config.get("output_file"));

			// file parsing
			int colSets = Integer.parseInt(String.valueOf(config.get("set_column"))) - 1;
			int colElements = Integer.parseInt(String.valueOf(config.get("elements_column"))) - 1;
			int colTokens = Integer.parseInt(String.valueOf(config.get("tokens_column"))) - 1;
			String colW = String.valueOf(config.get("weights_column"));
			int colWeights = colW.equals("null") ? -1 : Integer.parseInt(colW) - 1;

			String columnDelimiter = String.valueOf(config.get("column_delimiter"));
			if (columnDelimiter.equals("null") || columnDelimiter.equals(""))
				columnDelimiter = " ";
			String tokenDelimiter = String.valueOf(config.get("token_delimiter"));
			if (tokenDelimiter.equals("null") || tokenDelimiter.equals(""))
				tokenDelimiter = " ";
			boolean header = Boolean.parseBoolean(String.valueOf(config.get("header")));

			/* EXECUTE THE OPERATION */
			long numMatches = 0;
			FuzzySetCollectionReader reader = new FuzzySetCollectionReader();
			ConcurrentLinkedQueue<MatchingPair> results = new ConcurrentLinkedQueue<MatchingPair>();

			long duration = System.nanoTime();
			Thread simjoinThread = null;

			GroupCollection<ArrayList<String>> collection1 = null;
			if (queryFile != null) {
				collection1 = reader.fromCSV(queryFile, colSets, colElements, colTokens, colWeights, columnDelimiter,
						tokenDelimiter, maxLines, header);
			}
			GroupCollection<ArrayList<String>> collection2 = reader.fromCSV(inputFile, colSets, colElements, colTokens,
					colWeights, columnDelimiter, tokenDelimiter, maxLines, header);
			duration = System.nanoTime() - duration;
			System.out.println("Read time: " + duration / 1000000000.0 + " sec.");

			FuzzySetSimJoin ssjoin = null;
			// THRESHOLD-JOIN
			if (joinType.equalsIgnoreCase("threshold")) {

				// specify the similarity threshold
				double threshold = Double.parseDouble(String.valueOf(config.get("threshold")));

				// SELF-JOIN
				if (queryFile == null) {
					ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_THRESHOLD, collection2, threshold, results);
				}
				// FOREIGN-JOIN
				else {
					ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_THRESHOLD, collection1, collection2, threshold,
							results);
				}
			}
			// KNN-JOIN
			if (joinType.equalsIgnoreCase("knn")) {

				// specify k
				int k = Integer.parseInt(String.valueOf(config.get("k")));

				// SELF-JOIN
				if (queryFile == null) {
					ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_KNN, collection2, k, results);
				}
				// FOREIGN-JOIN
				else {
					ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_KNN, collection1, collection2, k, results);
				}
			}
			// TOPK-JOIN
			if (joinType.equalsIgnoreCase("topk")) {

				// specify k
				int k = Integer.parseInt(String.valueOf(config.get("k")));

				// SELF-JOIN
				if (queryFile == null) {
					ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_TOPK, collection2, k, results);
				}
				// FOREIGN-JOIN
				else {
					ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_TOPK, collection1, collection2, k, results);
				}
			}
			simjoinThread = new Thread(ssjoin);
			simjoinThread.setName("SimJoin");
			simjoinThread.start();

			// OUTPUT RESULTS
			MatchingPair result;
			PrintStream outStream = new PrintStream(outputFile);

			while (simjoinThread.isAlive() || !results.isEmpty()) {
				while (!results.isEmpty()) {
					result = results.poll();
					outStream.println(result);
					numMatches++;
				}
				TimeUnit.MILLISECONDS.sleep(10);
			}
			outStream.flush();
			outStream.close();
			System.out.println("Number of matches: " + numMatches);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}