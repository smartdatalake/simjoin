package simjoin;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import simjoin.sets.JoinResult;
import simjoin.sets.SetSimJoinImpl;
import simjoin.sets.TokenSet;
import simjoin.sets.TokenSetCollection;
import simjoin.sets.io.TokenSetCollectionReader;
import simjoin.sets.io.ResultsWriter;

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
			int colSets = Integer.parseInt(prop.getProperty("set_column")) - 1;
			int colTokens = Integer.parseInt(prop.getProperty("tokens_column")) - 1;
			String columnDelimiter = prop.getProperty("column_delimiter");
			if (columnDelimiter == null || columnDelimiter.equals(""))
				columnDelimiter = " ";
			String tokenDelimiter = prop.getProperty("token_delimiter");
			if (tokenDelimiter == null || tokenDelimiter.equals(""))
				tokenDelimiter = " ";

			// output
			boolean returnCounts = Boolean.parseBoolean(prop.getProperty("return_counts"));
			String outputFile = prop.getProperty("output_file");

			// similarity
			double simThreshold = Double.parseDouble(prop.getProperty("sim_threshold"));

			/* EXECUTE THE OPERATION */

			long duration;
			JoinResult result;
			ResultsWriter resultsWriter = new ResultsWriter();

			if (mode.equalsIgnoreCase("standard")) {

				TokenSetCollectionReader reader = new TokenSetCollectionReader();
				SetSimJoinImpl ssjoin = new SetSimJoinImpl();
				TokenSetCollection queryCollection, collection;

				switch (operation) {

				case "search":
					duration = System.nanoTime();
					queryCollection = reader.importFromFile(queryFile, colSets, colTokens, columnDelimiter,
							tokenDelimiter, maxLines);
					TokenSetCollection inputCollection = reader.importFromFile(inputFile, colSets, colTokens,
							columnDelimiter, tokenDelimiter, maxLines);
					TokenSet querySet = queryCollection.sets[queryId];
					duration = System.nanoTime() - duration;
					System.out.println("Read time: " + duration / 1000000000.0 + " sec.");

					result = ssjoin.rangeSearch(querySet, inputCollection, simThreshold, returnCounts);
					resultsWriter.printJoinResults(result, outputFile);
					break;

				case "self-join":
					duration = System.nanoTime();
					collection = reader.importFromFile(inputFile, colSets, colTokens, columnDelimiter, tokenDelimiter,
							maxLines);
					duration = System.nanoTime() - duration;
					System.out.println("Read time: " + duration / 1000000000.0 + " sec.");

					result = ssjoin.rangeSelfJoin(collection, simThreshold, returnCounts);
					resultsWriter.printJoinResults(result, outputFile);
					break;

				case "join":
					duration = System.nanoTime();
					queryCollection = reader.importFromFile(queryFile, colSets, colTokens, columnDelimiter,
							tokenDelimiter, maxLines);
					collection = reader.importFromFile(inputFile, colSets, colTokens, columnDelimiter, tokenDelimiter,
							maxLines);
					duration = System.nanoTime() - duration;
					System.out.println("Read time: " + duration / 1000000000.0 + " sec.");

					result = ssjoin.rangeJoin(queryCollection, collection, simThreshold, returnCounts);
					resultsWriter.printJoinResults(result, outputFile);
					break;

				default:
					System.out.println("Unknown operation");
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}