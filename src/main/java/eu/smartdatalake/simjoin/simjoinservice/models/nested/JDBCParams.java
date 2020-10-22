package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import org.json.simple.JSONObject;

import io.swagger.annotations.ApiModelProperty;

public class JDBCParams {

	@ApiModelProperty(notes = "Name of the Database.")
	public String db;
	
	@ApiModelProperty(notes = "User to login.")
	public String user;
	
	@ApiModelProperty(notes = "Password to login.")
	public String pwd;
	
	@ApiModelProperty(notes = "URL of the database.")
	public String url;
	
	@ApiModelProperty(notes = "CharSet of the Table.  (Optional)")
	public String charSet;
	
	@ApiModelProperty(notes = "Name of the Key Column.")
	public String keyCol;
	
	@ApiModelProperty(notes = "Name of the Tokens Column.")
	public String tokensCol;
	
	@ApiModelProperty(notes = "Name of the Weights Column (Optional)")
	public String weightsCol;

	public void addToConfig(JSONObject config) {
		config.put("db", db);
		config.put("user", user);
		config.put("pwd", pwd);
		config.put("url", url);
		config.put("charSet", charSet);
		config.put("keyCol", keyCol);
		config.put("tokensCol", tokensCol);
		config.put("weightsCol", weightsCol);
	}
}