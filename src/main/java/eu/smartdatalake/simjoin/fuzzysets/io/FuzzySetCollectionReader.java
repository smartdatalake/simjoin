package eu.smartdatalake.simjoin.fuzzysets.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollection;

public class FuzzySetCollectionReader { 

	/** Reads input from a CSV file */
	public FuzzySetCollection importFromFile(String file, int colSetId, int colElemId, int colSetTokens,
			String columnDelimiter, String tokenDelimiter, int maxLines, boolean header) {

//		public Map<String, List<Set<String>>> importCollectionFromFile(String file, int setCol, int tokenCol,
//				String columnDelimiter, String tokenDelimiter, boolean header, int maxLines) {

		Map<String, List<Set<String>>> collection = new LinkedHashMap<String, List<Set<String>>>();

		BufferedReader br;
		int lines = 0;
		int errorLines = 0;
		try {
			br = new BufferedReader(new FileReader(file));

			String line, set;
			String[] columns;
			Set<String> tokens;
			List<Set<String>> elements;

			// if the file has header, ignore the first line
			if (header) {
				br.readLine();
			}

			while ((line = br.readLine()) != null) {
				try {
					columns = line.split(columnDelimiter);

					set = columns[colSetId];
					tokens = new HashSet<String>(Arrays.asList(columns[colSetTokens].split(tokenDelimiter)));

					elements = collection.get(set);
					if (elements == null) {
						elements = new ArrayList<Set<String>>();
					}
					elements.add(tokens);
					collection.put(set, elements);
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

		System.out.println("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet);

		return new FuzzySetCollection(collection);
	}
}