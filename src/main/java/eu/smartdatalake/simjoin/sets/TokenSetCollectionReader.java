package eu.smartdatalake.simjoin.sets;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

/**
 * Creates a {@link GroupCollection} from the given input.
 *
 */
public class TokenSetCollectionReader {

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param file         Path to a CSV file.
	 * @param colSetId     The column containing the set identifiers.
	 * @param colSetTokens The column containing the set tokens.
	 * @param colWeights   The column containing the set weights.
	 * @param colDelimiter The column delimiter.
	 * @param tokDelimiter The token delimiter.
	 * @param maxLines     Number of lines to import (for debugging purposes).
	 * @param header       Whether the CSV file contains a header.
	 * @return A {@link GroupCollection}.
	 */
	private static final Logger logger = LogManager.getLogger(TokenSetCollectionReader.class);

	public GroupCollection<String> fromCSV(String file, int colSetId, int colSetTokens, int colWeights,
			String colDelimiter, String tokDelimiter, int maxLines, boolean header, String tokenizer, int qgram) {

		GroupCollection<String> collection = new GroupCollection<String>();
		collection.groups = new ArrayList<Group<String>>();
		int lineCount = 0, errorLines = 0;
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			String[] columns;
			Group<String> set;

			// if the file has header, ignore the first line
			if (header) {
				br.readLine();
			}

			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && lineCount >= maxLines) {
					break;
				}
				try {
					columns = line.split(colDelimiter);
					set = new Group<String>();
					set.id = columns[colSetId];
					set.elements = new ArrayList<String>();
					set.weight = (colWeights == -1) ? 1.0 : Double.parseDouble(columns[colWeights]);
					if (set.weight >= maxWeight)
						maxWeight = set.weight;
					if (set.weight <= minWeight)
						minWeight = set.weight;

					TObjectIntMap<String> tokens = new TObjectIntHashMap<String>();
					
					if (tokenizer.equals("qgram")) {
						String token = columns[colSetTokens];
						for (int i=0; i<=token.length()-qgram; i++) {
							tokens.adjustOrPutValue(token.substring(i, i+qgram), 1, 0);
						}
					} else {
						for (String tok: Arrays.asList(columns[colSetTokens].split(tokDelimiter))) {
							tokens.adjustOrPutValue(tok, 1, 0);
						}
					}

					for (String key: tokens.keySet()) {
						for (int val=0; val<=tokens.get(key); val++) {
							set.elements.add(key+"@"+val);
						}
					}
					collection.groups.add(set);
					lineCount++;
				} catch (Exception e) {
					errorLines++;
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		double elementsPerSet = 0;
		for (Group<String> set : collection.groups) {
			elementsPerSet += set.elements.size();
			if (colWeights != -1)
				set.weight = (set.weight - minWeight) / (maxWeight - minWeight);
		}
		elementsPerSet /= collection.groups.size();

		logger.info("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: " + errorLines
				+ ". Num of sets: " + collection.groups.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}
}