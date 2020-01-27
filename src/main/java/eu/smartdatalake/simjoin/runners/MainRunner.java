package eu.smartdatalake.simjoin.runners;

import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * The entry point of the execution. Execution parameters are provided in the
 * file config.json or another file specified as argument.
 *
 */
public class MainRunner {

	public static void main(String[] args) {

		try {
			String configFile = args.length > 0 ? args[0] : "config.json";

			/* READ PARAMETERS */
			JSONParser jsonParser = new JSONParser();
			JSONObject config = (JSONObject) jsonParser.parse(new FileReader(configFile));

			// operation
			String mode = String.valueOf(config.get("mode"));

			if (mode.equalsIgnoreCase("standard")) {
				RunSetSimJoin runner = new RunSetSimJoin();
				runner.execute(config);
			} else if (mode.equalsIgnoreCase("fuzzy")) {
				RunFuzzySetSimJoin runner = new RunFuzzySetSimJoin();
				runner.execute(config);
			} else {
				System.out.println("Unknown mode");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}