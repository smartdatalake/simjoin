package eu.smartdatalake.simjoin.fuzzysets;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

/**
 * Creates a {@link FuzzySetCollectionReader} from the given input.
 *
 */
public class FuzzySetCollectionReader {

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param file            Path to a CSV file.
	 * @param colSetId        The column containing the set identifiers.
	 * @param colElemId       The column containing the element identifiers.
	 * @param colSetTokens    The column containing the set tokens.
	 * @param colWeights      The column containing the set weights.
	 * @param columnDelimiter The column delimiter.
	 * @param tokenDelimiter  The token delimiter.
	 * @param maxLines        Number of lines to import (for debugging purposes).
	 * @param header          Whether the CSV file contains a header.
	 * @return A {@link GroupCollection}.
	 */
	private static final Logger logger = LogManager.getLogger(FuzzySetCollectionReader.class);

	public GroupCollection<ArrayList<String>> fromCSV(String file, int colSetId, int colElemId, int colSetTokens,
			int colWeights, String columnDelimiter, String tokenDelimiter, int maxLines, boolean header,
			String tokenizer, int qgram) {

		Map<String, ArrayList<ArrayList<String>>> collection = new LinkedHashMap<String, ArrayList<ArrayList<String>>>();
		TObjectDoubleMap<String> weights = new TObjectDoubleHashMap<String>();
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		double tokensPerElement = 0;
		BufferedReader br;
		int lines = 0;
		int errorLines = 0;
		try {
			br = new BufferedReader(new FileReader(file));

			String line, set;
			String[] columns;
			ArrayList<ArrayList<String>> elements;

			// if the file has header, ignore the first line
			if (header) {
				br.readLine();
			}

			while ((line = br.readLine()) != null) {
				try {
					columns = line.split(columnDelimiter);

					set = columns[colSetId];

					TObjectIntMap<String> tokens = new TObjectIntHashMap<String>();
					
					if (tokenizer.equals("qgram")) {
						String token = columns[colSetTokens];
						for (int i=0; i<=token.length()-qgram; i++) {
							tokens.adjustOrPutValue(token.substring(i, i+qgram), 1, 0);
						}
					} else {
						for (String tok: Arrays.asList(columns[colSetTokens].split(tokenDelimiter))) {
							tokens.adjustOrPutValue(tok, 1, 0);
						}
					}

					ArrayList<String> tokens2 = new ArrayList<String>();
					for (String key: tokens.keySet()) {
						for (int val=0; val<=tokens.get(key); val++) {
							tokens2.add(key+"@"+val);
						}
					}
					
					tokensPerElement += tokens2.size();
					elements = collection.get(set);
					if (elements == null) {
						elements = new ArrayList<ArrayList<String>>();
					}
					elements.add(tokens2);
					collection.put(set, elements);
					
					double weight = (colWeights == -1) ? 1.0 : Double.parseDouble(columns[colWeights]);
					weights.adjustOrPutValue(set, weight, weight);

					lines++;
				} catch (Exception e) {
					errorLines++;
				}
				if (maxLines > -1 && lines >= maxLines) {
					break;
				}
			}

			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		double elementsPerSet = 0;
		for (String set : collection.keySet()) {
			elementsPerSet += collection.get(set).size();
			double weight = weights.get(set);
			if (weight >= maxWeight)
				maxWeight = weight;
			if (weight <= minWeight)
				minWeight = weight;
		}
		elementsPerSet /= collection.size();
		tokensPerElement /= lines;

		// transform to group collection
		GroupCollection<ArrayList<String>> groupCollection = new GroupCollection<ArrayList<String>>();
		groupCollection.groups = new ArrayList<Group<ArrayList<String>>>();
		Group<ArrayList<String>> group;
		for (String key : collection.keySet()) {
			group = new Group<ArrayList<String>>();
			group.id = key;
			group.elements = collection.get(key);
			if (colWeights != -1) {
				group.weight = weights.get(key);
				group.weight = (weights.get(key) - minWeight) / (maxWeight - minWeight);
			} else
				group.weight = 1.0;

			groupCollection.groups.add(group);
		}

		logger.info("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: " + errorLines
				+ ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet
				+ ". Tokens per Element: " + tokensPerElement);

		return groupCollection;
	}
}