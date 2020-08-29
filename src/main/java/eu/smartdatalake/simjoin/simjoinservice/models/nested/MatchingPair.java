package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import io.swagger.annotations.ApiModelProperty;

public class MatchingPair {
	@ApiModelProperty(notes = "Left id")
	public String leftID;
	
	@ApiModelProperty(notes = "Right id")
	public String rightID;

	@ApiModelProperty(notes = "Pair score")
	public double score;
}