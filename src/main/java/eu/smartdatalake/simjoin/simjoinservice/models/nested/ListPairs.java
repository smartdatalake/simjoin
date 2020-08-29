package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import io.swagger.annotations.ApiModelProperty;

public class ListPairs {
	@ApiModelProperty(notes = "A ranked list")
	public MatchingPair[] rankedResults;
}
