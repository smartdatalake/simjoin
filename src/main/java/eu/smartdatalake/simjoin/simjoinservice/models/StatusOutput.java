package eu.smartdatalake.simjoin.simjoinservice.models;

import java.util.ArrayList;
import java.util.List;

import io.swagger.annotations.ApiModelProperty;

public class StatusOutput {
	@ApiModelProperty(notes = "The list of results")
	public List<String> pairs;
	
	@ApiModelProperty(notes = "The status of the job")
	public String status;
	
	@ApiModelProperty(notes = "The logs of the job")
	public String log;
	
	@ApiModelProperty(notes = "The timestamp of the creation of the job")
	public String date;
	
	public StatusOutput() {
		pairs = new ArrayList<String>();
	}
}