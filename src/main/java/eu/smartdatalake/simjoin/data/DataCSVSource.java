package eu.smartdatalake.simjoin.data;

import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.data.prepared.PreparedFuzzySet;
import eu.smartdatalake.simjoin.data.prepared.PreparedStandardSet;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollectionReader;
import eu.smartdatalake.simjoin.sets.TokenSetCollectionReader;

/**
 * Creates a {@link DataCSVSource} from the given input.
 *
 */
public class DataCSVSource extends DataSource {
	public String file;
	public int colSetId;
	public int colSetTokens;
	public int colWeights;
	public String columnDelimiter;
	public boolean header;

	/**
	 * Creates a {@link DataCSVSource} from a CSV file.
	 * 
	 * @param config The configuration of the input
	 * @param mode   The mode of the input, i.e. Standard or Fuzzy
	 * @return A {@link DataCSVSource}.
	 */
	public DataCSVSource(JSONObject config, String mode) {
		super(config, mode, "csv");

		file = String.valueOf(config.get("file"));

		colSetId = Integer.parseInt(String.valueOf(config.get("colSetId"))) - 1;
		colSetTokens = Integer.parseInt(String.valueOf(config.get("colSetTokens"))) - 1;
		String colW = String.valueOf(config.get("colWeights"));
		colWeights = colW.equals("null") ? -1 : Integer.parseInt(colW) - 1;

		columnDelimiter = String.valueOf(config.get("columnDelimiter"));
		if (columnDelimiter.equals("null") || columnDelimiter.equals(""))
			columnDelimiter = " ";
		header = Boolean.parseBoolean(String.valueOf(config.get("header")));
	}

	/**
	 * Creates a {@link GroupCollection} from the {@link DataCSVSource}.
	 * 
	 * @param maxLines Number of lines to parse from the file.
	 * @return A {@link GroupCollection}.
	 */
	public GroupCollection getData(int maxLines) {
		if (mode.equals("fuzzy"))
			return FuzzySetCollectionReader.fromCSV(this, maxLines);
		else
			return TokenSetCollectionReader.fromCSV(this, maxLines);
	}
	
	/**
	 * Prepares the {@link DataSource} by parsing, transforming and creating the index.
	 * 
	 * @param maxLines Number of lines to parse.
	 * @param threshold Threshold for index (only in Standard ThresholdJoin
	 */
	public void prepare(int maxLines, double threshold) {
		if (mode.equals("standard")) {
			prepared = new PreparedStandardSet(TokenSetCollectionReader.fromCSV(this, maxLines), threshold);	
		} else if (mode.equals("fuzzy")) {
			prepared = new PreparedFuzzySet(FuzzySetCollectionReader.fromCSV(this, maxLines));
		}
	}
}
