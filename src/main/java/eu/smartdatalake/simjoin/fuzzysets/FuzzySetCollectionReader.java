package eu.smartdatalake.simjoin.fuzzysets;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;

/**
 * Creates a {@link FuzzySetCollectionReader} from the given input.
 *
 */
public class FuzzySetCollectionReader {

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param file
	 *            Path to a CSV file.
	 * @param colSetId
	 *            The column containing the set identifiers.
	 * @param colElemId
	 *            The column containing the element identifiers.
	 * @param colSetTokens
	 *            The column containing the set tokens.
	 * @param colWeights
	 *            The column containing the set weights.
	 * @param columnDelimiter
	 *            The column delimiter.
	 * @param tokenDelimiter
	 *            The token delimiter.
	 * @param maxLines
	 *            Number of lines to import (for debugging purposes).
	 * @param header
	 *            Whether the CSV file contains a header.
	 * @return A {@link GroupCollection}.
	 */
	public GroupCollection<ArrayList<String>> fromCSV(String file, int colSetId, int colElemId, int colSetTokens,
			int colWeights, String columnDelimiter, String tokenDelimiter, int maxLines, boolean header) {

		Map<String, ArrayList<ArrayList<String>>> collection = new LinkedHashMap<String, ArrayList<ArrayList<String>>>();
		TObjectDoubleMap<String> weights = new TObjectDoubleHashMap<String>();

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

					Set<String> tokens2 = new HashSet<String>();
					tokens2.addAll(Arrays.asList(columns[colSetTokens].split(tokenDelimiter)));
					ArrayList<String> tokens = new ArrayList<String>();
					tokens.addAll(tokens2);

					tokensPerElement += tokens.size();

					elements = collection.get(set);
					if (elements == null) {
						elements = new ArrayList<ArrayList<String>>();
					}
					elements.add(tokens);
					collection.put(set, elements);
					double weight = (colWeights == -1) ? 1.0 : Integer.parseInt(columns[colWeights]);
					weights.put(set, weight);
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
			group.weight = weights.get(key);
			groupCollection.groups.add(group);
		}

		System.out.println("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet
				+ ". Tokens per Element: " + tokensPerElement);

		return groupCollection;
	}
}