package eu.smartdatalake.simjoin;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyJoinResult;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySet;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollection;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetSimJoinImpl;
import eu.smartdatalake.simjoin.fuzzysets.io.FuzzyResultsWriter;
import eu.smartdatalake.simjoin.fuzzysets.io.FuzzySetCollectionReader;
import eu.smartdatalake.simjoin.sets.JoinResult;
import eu.smartdatalake.simjoin.sets.SetSimJoinImpl;
import eu.smartdatalake.simjoin.sets.TokenSet;
import eu.smartdatalake.simjoin.sets.TokenSetCollection;
import eu.smartdatalake.simjoin.sets.io.TokenSetCollectionReader;
import eu.smartdatalake.simjoin.sets.io.ResultsWriter;

public class RunSimJoin {

	public static void main(String[] args) {

		String configFile = args.length > 0 ? args[0] : "config.properties";

		try (InputStream config = new FileInputStream(configFile)) {

			/* READ PARAMETERS */
			Properties prop = new Properties();
			prop.load(config);

			// mode and operation
			String mode = prop.getProperty("mode");
			String operation = prop.getProperty("operation");

			// input
			String inputFile = prop.getProperty("input_file");
			String queryFile = prop.getProperty("query_file");
			int queryId = Integer.parseInt(prop.getProperty("query_id"));
			int maxLines = Integer.parseInt(prop.getProperty("max_lines"));

			// file parsing
			int colFuzzySets = Integer.parseInt(prop.getProperty("fuzzyset_column")) - 1;
			int colSets = Integer.parseInt(prop.getProperty("set_column")) - 1;
			int colTokens = Integer.parseInt(prop.getProperty("tokens_column")) - 1;
			String columnDelimiter = prop.getProperty("column_delimiter");
			if (columnDelimiter == null || columnDelimiter.equals(""))
				columnDelimiter = " ";
			String tokenDelimiter = prop.getProperty("token_delimiter");
			if (tokenDelimiter == null || tokenDelimiter.equals(""))
				tokenDelimiter = " ";
			boolean header = Boolean.parseBoolean(prop.getProperty("header"));

			// output
			boolean returnCounts = Boolean.parseBoolean(prop.getProperty("return_counts"));
			String outputFile = prop.getProperty("output_file");

			// similarity
			double simThreshold = Double.parseDouble(prop.getProperty("sim_threshold"));

			// top-k
			int k = Integer.parseInt(prop.getProperty("k"));

			/* EXECUTE THE OPERATION */

			long duration;

			if (mode.equalsIgnoreCase("standard")) {
				JoinResult result = null;
				ResultsWriter resultsWriter = new ResultsWriter();

				TokenSetCollectionReader reader = new TokenSetCollectionReader();
				SetSimJoinImpl ssjoin = new SetSimJoinImpl();
				TokenSetCollection queryCollection = null, collection;
				TokenSet querySet = null;

				duration = System.nanoTime();
				collection = reader.importFromFile(inputFile, colSets, colTokens, columnDelimiter, tokenDelimiter,
						maxLines, header);

				if (!operation.contains("self")) {
					queryCollection = reader.importFromFile(queryFile, colSets, colTokens, columnDelimiter,
							tokenDelimiter, maxLines, header);
					duration = System.nanoTime() - duration;
					System.out.println("Read time: " + duration / 1000000000.0 + " sec.");
				}
				switch (operation) {

				case "search":
					querySet = queryCollection.sets[queryId];
					result = ssjoin.rangeSearch(querySet, collection, simThreshold, returnCounts);
					break;

				case "self-join":
					result = ssjoin.rangeSelfJoin(collection, simThreshold, returnCounts);
					break;

				case "join":
					result = ssjoin.rangeJoin(queryCollection, collection, simThreshold, returnCounts);
					break;

				case "knn-search":
					querySet = queryCollection.sets[queryId];
					result = ssjoin.knnSearch(querySet, collection, k);
					break;

				case "knn-join":
					result = ssjoin.knnJoin(queryCollection, collection, k);
					break;

				case "self-closest-pairs":
					result = ssjoin.closestPairsSelfJoin(collection, k);
					break;

				case "closest-pairs":
					result = ssjoin.closestPairsJoin(queryCollection, collection, k);
					break;

				default:
					System.out.println("Unknown operation");
					break;
				}
				result.leftSize = (querySet == null)
						? (queryCollection == null) ? collection.sets.length : queryCollection.sets.length : 1;
				result.rightSize = collection.sets.length;
				resultsWriter.printJoinResults(result, outputFile, operation.contains("self"));
			}

			else if (mode.equalsIgnoreCase("fuzzy")) {
				FuzzyJoinResult result = null;
				FuzzyResultsWriter resultsWriter = new FuzzyResultsWriter();

				FuzzySetCollectionReader reader = new FuzzySetCollectionReader();
				FuzzySetSimJoinImpl ssjoin = new FuzzySetSimJoinImpl();
				FuzzySetCollection queryCollection = null, collection;
				FuzzySet querySet = null;

				duration = System.nanoTime();
				collection = reader.importFromFile(inputFile, colFuzzySets, colSets, colTokens, columnDelimiter,
						tokenDelimiter, maxLines, header);
				if (!operation.contains("self")) {
					queryCollection = reader.importFromFile(queryFile, colFuzzySets, colSets, colTokens,
							columnDelimiter, tokenDelimiter, maxLines, header);
					duration = System.nanoTime() - duration;
					System.out.println("Read time: " + duration / 1000000000.0 + " sec.");
				}
				switch (operation) {

				case "search":
					querySet = queryCollection.getSet(queryId);
					result = ssjoin.rangeSearch(querySet, collection, simThreshold, returnCounts);
					break;

				case "self-join":
					result = ssjoin.rangeSelfJoin(collection, simThreshold, returnCounts);
					break;

				case "join":
					result = ssjoin.rangeJoin(queryCollection, collection, simThreshold, returnCounts);
					break;

				default:
					System.out.println("Unknown operation");
					break;
				}
				result.leftSize = (querySet == null)
						? (queryCollection == null) ? collection.sets.size() : queryCollection.sets.size() : 1;
				result.rightSize = collection.sets.size();
				resultsWriter.printJoinResults(result, outputFile, operation.contains("self"));
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}