package eu.smartdatalake.simjoin.sets;

import java.util.Set;

public class TokenSet {
	public String id;
	public Set<String> tokens;
	
	@Override
	public String toString() {
		return id+": "+tokens.toString();
	}
	
	
}