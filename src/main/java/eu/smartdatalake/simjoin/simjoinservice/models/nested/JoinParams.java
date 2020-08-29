package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import org.json.simple.JSONObject;

import io.swagger.annotations.ApiModelProperty;

public class JoinParams {

	@ApiModelProperty(notes = "The number corresponding to config file")
	public int config_file;

	@ApiModelProperty(notes = "The type of join (threshold, knn, topk")
	public String join_type;

	@ApiModelProperty(notes = "The threshold to use")
	public double threshold;

	@ApiModelProperty(notes = "The number of results if knn or topk")
	public int k;

	public void addToConfig(JSONObject config) {
		config.put("join_type", join_type);
		config.put("threshold", threshold);
		config.put("k", k);
	}
}