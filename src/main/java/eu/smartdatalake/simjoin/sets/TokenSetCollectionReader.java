package eu.smartdatalake.simjoin.sets;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;

/**
 * Creates a {@link GroupCollection} from the given input.
 *
 */
public class TokenSetCollectionReader {

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param file
	 *            Path to a CSV file.
	 * @param colSetId
	 *            The column containing the set identifiers.
	 * @param colSetTokens
	 *            The column containing the set tokens.
	 * @param colWeights
	 *            The column containing the set weights.
	 * @param colDelimiter
	 *            The column delimiter.
	 * @param tokDelimiter
	 *            The token delimiter.
	 * @param maxLines
	 *            Number of lines to import (for debugging purposes).
	 * @param header
	 *            Whether the CSV file contains a header.
	 * @return A {@link GroupCollection}.
	 */
	public GroupCollection<String> fromCSV(String file, int colSetId, int colSetTokens, int colWeights,
			String colDelimiter, String tokDelimiter, int maxLines, boolean header) {

		GroupCollection<String> collection = new GroupCollection<String>();
		collection.groups = new ArrayList<Group<String>>();
		int lineCount = 0, errorLines = 0;

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
					set.weight = (colWeights == -1) ? 1.0 : Integer.parseInt(columns[colWeights]);

					// only allow distinct tokens
					Set<String> tokens = new HashSet<String>();
					tokens.addAll(Arrays.asList(columns[colSetTokens].split(tokDelimiter)));

					set.elements.addAll(tokens);
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
		}
		elementsPerSet /= collection.groups.size();

		System.out.println("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.groups.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}
}