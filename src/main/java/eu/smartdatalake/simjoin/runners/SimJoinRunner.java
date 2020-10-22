package eu.smartdatalake.simjoin.runners;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.data.DataSource;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetSimJoin;
import eu.smartdatalake.simjoin.sets.SetSimJoin;

/**
 * Executes set similarity join operations according to the provided
 * configuration parameters.
 *
 */
public class SimJoinRunner extends Thread {
	private static final Logger logger = LogManager.getLogger(SimJoinRunner.class);
//	private boolean stayAlive = true;
	Thread simjoinThread = null;
	ConcurrentLinkedQueue<MatchingPair> results = null;
	String outputFile;
	long timeout;
	
	public SimJoinRunner(DataSource ds1, DataSource ds2, JSONObject configJoin, String mode, long timeout) {
		this.timeout = timeout;
		if (mode.equalsIgnoreCase("standard")) {
			executeStandard(ds1, ds2, configJoin);
		} else if (mode.equalsIgnoreCase("fuzzy")) {
			executeFuzzy(ds1, ds2, configJoin);
		}
	}

	public SimJoinRunner(DataSource ds1, DataSource ds2, JSONObject configJoin, String mode) {
		this(ds1, ds2, configJoin, mode, -1);
	}

	private void executeStandard(DataSource ds1, DataSource ds2, JSONObject configJoin) {
		try {
			/* READ PARAMETERS */
			// operation
			String joinType = String.valueOf(configJoin.get("join_type"));

			int maxLines = Integer.parseInt(String.valueOf(configJoin.get("max_lines")));
			outputFile = String.valueOf(configJoin.get("output_file"));

			/* EXECUTE THE OPERATION */
			if (!outputFile.equals("null"))
				results = new ConcurrentLinkedQueue<MatchingPair>();

			long duration = System.nanoTime();

			GroupCollection<String> collection1 = null;
			if (ds1 != null) {
				collection1 = ds1.getData(maxLines);
			}

			GroupCollection<String> collection2 = ds2.getData(maxLines);

			duration = System.nanoTime() - duration;
			logger.info("Read time: " + duration / 1000000000.0 + " sec.");

			SetSimJoin ssjoin = null;

			// THRESHOLD-JOIN
			if (joinType.equalsIgnoreCase("threshold")) {

				// specify the similarity threshold
				double threshold = Double.parseDouble(String.valueOf(configJoin.get("threshold")));

				// SELF-JOIN
				if (collection1 == null) {
					ssjoin = new SetSimJoin(SetSimJoin.TYPE_THRESHOLD, collection2, threshold, results);
				}
				// FOREIGN-JOIN
				else {
					ssjoin = new SetSimJoin(SetSimJoin.TYPE_THRESHOLD, collection1, collection2, threshold, results);
				}
			}
			// KNN-JOIN
			if (joinType.equalsIgnoreCase("knn")) {

				// specify k
				int k = Integer.parseInt(String.valueOf(configJoin.get("k")));
				try {
					double threshold = Double.parseDouble(String.valueOf(configJoin.get("threshold")));

					if (collection1 == null) {
						ssjoin = new SetSimJoin(SetSimJoin.TYPE_KNN, collection2, k, threshold, results);
					}
					// FOREIGN-JOIN
					else {
						ssjoin = new SetSimJoin(SetSimJoin.TYPE_KNN, collection1, collection2, k, threshold, results);
					}
				} catch (Exception e) {
					// SELF-JOIN
					if (collection1 == null) {
						ssjoin = new SetSimJoin(SetSimJoin.TYPE_KNN, collection2, k, results);
					}
					// FOREIGN-JOIN
					else {
						ssjoin = new SetSimJoin(SetSimJoin.TYPE_KNN, collection1, collection2, k, results);
					}
				}

			}
			// TOPK-JOIN
			if (joinType.equalsIgnoreCase("topk")) {

				// specify k
				int k = Integer.parseInt(String.valueOf(configJoin.get("k")));

				// SELF-JOIN
				if (collection1 == null) {
					ssjoin = new SetSimJoin(SetSimJoin.TYPE_TOPK, collection2, k, results);
				}
				// FOREIGN-JOIN
				else {
					ssjoin = new SetSimJoin(SetSimJoin.TYPE_TOPK, collection1, collection2, k, results);
				}
			}

			ssjoin.timeout = timeout;
			simjoinThread = new Thread(ssjoin);
			simjoinThread.setName("SimJoin");

		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}

	private void executeFuzzy(DataSource ds1, DataSource ds2, JSONObject configJoin) {
		try {
			/* READ PARAMETERS */
			// operation
			String joinType = String.valueOf(configJoin.get("join_type"));

			int maxLines = Integer.parseInt(String.valueOf(configJoin.get("max_lines")));
			outputFile = String.valueOf(configJoin.get("output_file"));

			/* EXECUTE THE OPERATION */
			if (!outputFile.equals("null"))
				results = new ConcurrentLinkedQueue<MatchingPair>();

			long duration = System.nanoTime();
			simjoinThread = null;

			GroupCollection<ArrayList<String>> collection1 = null;
			if (ds1 != null) {
				collection1 = ds1.getData(maxLines);
			}

			GroupCollection<ArrayList<String>> collection2 = ds2.getData(maxLines);

			duration = System.nanoTime() - duration;
			logger.info("Read time: " + duration / 1000000000.0 + " sec.");

			FuzzySetSimJoin ssjoin = null;
			// THRESHOLD-JOIN
			if (joinType.equalsIgnoreCase("threshold")) {

				// specify the similarity threshold
				double threshold = Double.parseDouble(String.valueOf(configJoin.get("threshold")));

				// SELF-JOIN
				if (collection1 == null) {
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
				int k = Integer.parseInt(String.valueOf(configJoin.get("k")));
				try {
					double threshold = Double.parseDouble(String.valueOf(configJoin.get("threshold")));
					// SELF-JOIN
					if (collection1 == null) {
						ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_KNN, collection2, k, threshold, results);
					}
					// FOREIGN-JOIN
					else {
						ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_KNN, collection1, collection2, k, threshold,
								results);
					}
				} catch (Exception e) {
					// SELF-JOIN
					if (collection1 == null) {
						ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_KNN, collection2, k, results);
					}
					// FOREIGN-JOIN
					else {
						ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_KNN, collection1, collection2, k, results);
					}
				}

			}
			// TOPK-JOIN
			if (joinType.equalsIgnoreCase("topk")) {

				// specify k
				int k = Integer.parseInt(String.valueOf(configJoin.get("k")));

				// SELF-JOIN
				if (collection1 == null) {
					ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_TOPK, collection2, k, results);
				}
				// FOREIGN-JOIN
				else {
					ssjoin = new FuzzySetSimJoin(FuzzySetSimJoin.TYPE_TOPK, collection1, collection2, k, results);
				}
			}
			ssjoin.timeout = timeout;
			
			simjoinThread = new Thread(ssjoin);
			simjoinThread.setName("SimJoin");
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		simjoinThread.start();
		
		long numMatches = 0;
		try {
			// OUTPUT RESULTS
			if (results != null) {
				MatchingPair result;
				PrintStream outStream = new PrintStream(outputFile);

				while ((simjoinThread.isAlive() || !results.isEmpty())) {
					while (!results.isEmpty()) {
						result = results.poll();
						outStream.println(result);
						numMatches++;
					}
					TimeUnit.MILLISECONDS.sleep(10);
				}
				outStream.flush();
				outStream.close();
				logger.info("Number of matches: " + numMatches);
				System.out.println("Number of matches: " + numMatches);
			} else {
				while (simjoinThread.isAlive()) {
					TimeUnit.MILLISECONDS.sleep(10);
				}
			}
		} catch (InterruptedException | FileNotFoundException e) {

		}
	}
}