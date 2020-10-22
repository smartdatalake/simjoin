package eu.smartdatalake.simjoin.sets;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.data.DataCSVSource;
import eu.smartdatalake.simjoin.data.DataFileReader;
import eu.smartdatalake.simjoin.data.DataJDBCSource;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

/**
 * Creates a {@link GroupCollection} from the given input.
 *
 */
public class TokenSetCollectionReader {

	private static final Logger logger = LogManager.getLogger(TokenSetCollectionReader.class);

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param ds       {@link DataCSVSource} Source to use for the data.
	 * @param maxLines Number of lines to parse from the file.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<String> fromCSV(DataCSVSource ds, int maxLines) {

		GroupCollection<String> collection = new GroupCollection<String>();
		collection.groups = new ArrayList<Group<String>>();
		int lineCount = 0, errorLines = 0;
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		try {
			DataFileReader dr = new DataFileReader(ds.file);
			String line;
			String[] columns;
			Group<String> set;

			// if the file has header, ignore the first line
			if (ds.header) {
				dr.readLine();
			}

			while ((line = dr.readLine()) != null) {
				if (maxLines > 0 && lineCount >= maxLines) {
					break;
				}
				try {
					columns = line.split(ds.columnDelimiter);
					set = new Group<String>();
					set.id = columns[ds.colSetId];

					set.weight = (ds.colWeights == -1) ? 1.0 : Double.parseDouble(columns[ds.colWeights]);
					maxWeight = (set.weight > maxWeight) ? set.weight : maxWeight;
					minWeight = (set.weight < minWeight) ? set.weight : minWeight;

					set.elements = getTokens(columns[ds.colSetTokens], ds.tokenizer, ds.qgram, ds.tokenDelimiter);
					collection.groups.add(set);
					lineCount++;
				} catch (Exception e) {
					errorLines++;
				}
			}
			dr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		double elementsPerSet = 0;
		for (Group<String> set : collection.groups) {
			elementsPerSet += set.elements.size();
			if (ds.colWeights != -1)
				set.weight = (set.weight - minWeight) / (maxWeight - minWeight);
		}
		elementsPerSet /= collection.groups.size();

		System.out.println("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.groups.size() + ". Elements per set: " + elementsPerSet);
		logger.info("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: " + errorLines
				+ ". Num of sets: " + collection.groups.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param ds       {@link DataJDBCSource} Source to use for the data.
	 * @param maxLines Number of lines to retrieve from the Database.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<String> fromJDBC(DataJDBCSource ds, int maxLines) {

		ResultSet resultSet = ds.executeStatement(maxLines);

		GroupCollection<String> collection = new GroupCollection<String>();
		collection.groups = new ArrayList<Group<String>>();
		int lines = 0;
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		try {
			while (resultSet.next()) {
				Group<String> set = new Group<String>();
				set.id = resultSet.getString(1);
				set.weight = (!ds.existsWeight()) ? 1.0 : Double.parseDouble(resultSet.getString(3));
				maxWeight = (set.weight > maxWeight) ? set.weight : maxWeight;
				minWeight = (set.weight < minWeight) ? set.weight : minWeight;

				set.elements = getTokens(resultSet.getString(2), ds.tokenizer, ds.qgram, ds.tokenDelimiter);
				collection.groups.add(set);
				lines++;
			}
			double elementsPerSet = 0;
			for (Group<String> set : collection.groups) {
				elementsPerSet += set.elements.size();
				if (ds.existsWeight())
					set.weight = (set.weight - minWeight) / (maxWeight - minWeight);
			}
			elementsPerSet /= collection.groups.size();

			System.out.println("Finished quering DB. Lines read: " + lines + ". Num of sets: " + collection.groups.size()
					+ ". Elements per set: " + elementsPerSet);
			logger.info("Finished quering DB. Lines read: " + lines + ". Num of sets: " + collection.groups.size()
					+ ". Elements per set: " + elementsPerSet);

			return collection;
		} catch (NumberFormatException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private static ArrayList<String> getTokens(String line, String tokenizer, int qgram, String tokenDelimiter) {

		TObjectIntMap<String> tokens = new TObjectIntHashMap<String>();
		if (tokenizer.equals("qgram")) {
			String token = line;
			for (int i = 0; i <= token.length() - qgram; i++) {
				tokens.adjustOrPutValue(token.substring(i, i + qgram), 1, 0);
			}
		} else {
			for (String tok : Arrays.asList(line.split(tokenDelimiter))) {
				tokens.adjustOrPutValue(tok, 1, 0);
			}
		}

		ArrayList<String> tokens2 = new ArrayList<String>();
		for (String key : tokens.keySet()) {
			for (int val = 0; val <= tokens.get(key); val++) {
				tokens2.add(key + "@" + val);
			}
		}

		return tokens2;
	}
}