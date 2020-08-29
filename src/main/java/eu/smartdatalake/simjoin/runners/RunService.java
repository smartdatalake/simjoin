package eu.smartdatalake.simjoin.runners;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.SpringApplication;

import eu.smartdatalake.simjoin.simjoinservice.ServiceApplication;

public class RunService {

	public RunService(String[] args)  {

		// LOAD VALID API KEYS
		String apiKeysFile = args.length > 1 ? args[1] : "valid_api_keys.json";

		JSONParser jsonParser = new JSONParser();
		JSONObject apiKeys;
		try {
			apiKeys = (JSONObject) jsonParser.parse(new FileReader(apiKeysFile));
			JSONArray keys = (JSONArray) apiKeys.get("valid_api_keys");
			String keysString = "";
			for (Object key : keys) {
				keysString += key + " ";
			}
			keysString = keysString.trim();

			System.setProperty("valid_api_keys", keysString);
			

			// START THE SERVICE
			SpringApplication.run(ServiceApplication.class, args);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}