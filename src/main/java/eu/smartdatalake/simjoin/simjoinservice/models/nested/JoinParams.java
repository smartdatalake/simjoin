package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import org.json.simple.JSONObject;

import io.swagger.annotations.ApiModelProperty;

public class JoinParams {

	@ApiModelProperty(notes = "The number of the input DataSource")
	public String input_dataSource;
	
	@ApiModelProperty(notes = "The number of the query DataSource (optional).")
	public String query_dataSource;

	@ApiModelProperty(notes = "The type of join (threshold, knn, topk")
	public String join_type;

	@ApiModelProperty(notes = "The threshold to use")
	public double threshold;

	@ApiModelProperty(notes = "The number of results if knn or topk")
	public int k;
	
	@ApiModelProperty(notes = "The number of lines to retrieve/parse from each data source.")
	public int max_lines;
	
	public void addToConfig(JSONObject config) {
		config.put("join_type", join_type);
		config.put("threshold", threshold);
		config.put("k", k);
		config.put("max_lines", max_lines);
	}
}