package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import io.swagger.annotations.ApiModelProperty;

public class Params {

	@ApiModelProperty(notes = "The number of results to return")
	public int limit;

	@ApiModelProperty(notes = "The join parameters")
	public JoinParams params;
}