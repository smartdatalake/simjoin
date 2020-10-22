package eu.smartdatalake.simjoin.simjoinservice.models;

import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.simjoinservice.models.nested.CSVParams;
import eu.smartdatalake.simjoin.simjoinservice.models.nested.JDBCParams;
import io.swagger.annotations.ApiModelProperty;

public class AddSourceInput {
	@ApiModelProperty(notes = "Nickname for the datasource, given by the user.")
	public String name;
	
	@ApiModelProperty(notes = "Type of the DataSource: jdbc or csv.")
	public String type;
	
	@ApiModelProperty(notes = "Mode of the DataSource: fuzzy or standard.")
	public String mode;
	
	@ApiModelProperty(notes = "Tokenizer to use: word or qgram")
	public String tokenizer;
	
	@ApiModelProperty(notes = "Token Delimiter used to split tokens (if tokenizer is word).")
	public String tokenDelimiter;
	
	@ApiModelProperty(notes = "Qgram to split tokens (if tokenizer is qgram).")
	public int qgram;

	@ApiModelProperty(notes = "Extra parameters for CSV.")
	public CSVParams csv;
	
	@ApiModelProperty(notes = "Extra parameters for JDBC.")
	public JDBCParams jdbc;

	public JSONObject toConfig() {
		JSONObject config = new JSONObject();
		config.put("type", type);
		config.put("mode", mode);
		config.put("tokenizer", tokenizer);
		config.put("tokenDelimiter", tokenDelimiter);
		config.put("qgram", qgram);
		
		if (type.equals("jdbc"))
			jdbc.addToConfig(config);
		else if (type.equals("csv"))
			csv.addToConfig(config);
		
		return config;
	}
}