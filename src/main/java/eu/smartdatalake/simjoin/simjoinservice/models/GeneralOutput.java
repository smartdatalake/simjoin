package eu.smartdatalake.simjoin.simjoinservice.models;

import io.swagger.annotations.ApiModelProperty;

public class GeneralOutput {
	@ApiModelProperty(notes = "Message")
	public String msg;
	
	@ApiModelProperty(notes = "ID for later use.")
	public String id;
	
	public GeneralOutput(String msg, String id ) {
		this.id = id;
		this.msg = msg;
	}
	
}