package eu.smartdatalake.simjoin.data;

import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.data.prepared.PreparedFuzzySet;
import eu.smartdatalake.simjoin.data.prepared.PreparedSet;
import eu.smartdatalake.simjoin.data.prepared.PreparedStandardSet;

/**
 * Creates a {@link DataSource} from the given input.
 *
 */
public class DataSource {
	public String type;
	public String mode;
	public String tokenDelimiter;
	public String tokenizer;
	public int qgram;
	public PreparedSet prepared;

	/**
	 * Creates a {@link DataSource} from a CSV file.
	 * 
	 * @param config The configuration of the input
	 * @param mode   The mode of the input, i.e. Standard or Fuzzy
	 * @param type   The type of the input, i.e CSV or JDBC
	 * @return A {@link DataSource}.
	 */
	public DataSource(JSONObject config, String mode, String type) {
		this.mode = mode;
		this.type = type;
		tokenDelimiter = String.valueOf(config.get("tokenDelimiter"));
		if (tokenDelimiter.equals("null") || tokenDelimiter.equals(""))
			tokenDelimiter = " ";
		tokenizer = String.valueOf(config.get("tokenizer"));
		if (tokenizer.equals("null") || tokenizer.equals(""))
			tokenizer = "word";
		qgram = Integer.parseInt(String.valueOf(config.get("qgram")));
	}

	/**
	 * Creates a {@link GroupCollection} from the {@link DataSource}.
	 * 
	 * @param maxLines Number of lines to parse from the file.
	 * @return A {@link GroupCollection}.
	 */
	public GroupCollection getData(int maxLines) {
		return null;
	}

	/**
	 * Prepares the {@link DataSource} by parsing, transforming and creating the
	 * index.
	 * 
	 * @param maxLines  Number of lines to parse.
	 * @param threshold Threshold for index (only in Standard ThresholdJoin
	 */
	public void prepare(int maxLines, double threshold) {
	}
}
