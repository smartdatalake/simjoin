package eu.smartdatalake.simjoin.runners;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import eu.smartdatalake.simjoin.data.DataCSVSource;
import eu.smartdatalake.simjoin.data.DataESSource;
import eu.smartdatalake.simjoin.data.DataJDBCSource;
import eu.smartdatalake.simjoin.data.DataJSONSource;
import eu.smartdatalake.simjoin.data.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

/**
 * The entry point of the execution. Execution parameters are provided in the
 * file config.json or another file specified as argument.
 *
 */
public class MainRunner {
	private static final Logger logger = LogManager.getLogger(MainRunner.class);

	public static void main(String[] args) {

		if (args.length > 0 && (args[0].equals("--service") || args[0].equals("-s")))
			new RunService(args);
		else {
			long duration = System.nanoTime();
			try {
				String configInputFile = "config.json";
				String configQueryFile = null;
				String configJoinFile = "config.json";

				for (int i = 0; i < args.length; i += 2) {
					if (args[i].equals("-i") || args[i].equals("--input"))
						configInputFile = args[i + 1];
					if (args[i].equals("-q") || args[i].equals("--query"))
						configQueryFile = args[i + 1];
					if (args[i].equals("-j") || args[i].equals("--join"))
						configJoinFile = args[i + 1];
				}

				/* READ PARAMETERS */
				JSONParser jsonParser = new JSONParser();
				JSONObject configInput = (JSONObject) jsonParser.parse(new FileReader(configInputFile));
				JSONObject configQuery = null;
				if (configQueryFile != null)
					configQuery = (JSONObject) jsonParser.parse(new FileReader(configQueryFile));
				JSONObject configJoin = (JSONObject) jsonParser.parse(new FileReader(configJoinFile));

				String logFile = String.valueOf(configJoin.get("log_file"));
				System.setProperty("logFilename", logFile);
				LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
				ctx.reconfigure();

				// operation
				DataSource ds1 = null;
				if (configQuery != null) {
					String mode = String.valueOf(configQuery.get("mode"));
					String dataSource = String.valueOf(configQuery.get("dataSource"));
//					String queryFile = String.valueOf(configQuery.get("query_file"));
//					if (!queryFile.equals("null") && !queryFile.equals("")) {
					if (dataSource.equals("csv"))
						ds1 = new DataCSVSource(configQuery, mode);
					else if (dataSource.equals("jdbc"))
						ds1 = new DataJDBCSource(configQuery, mode);
					else if (dataSource.equals("es"))
						ds1 = new DataESSource(configQuery, mode);
					else if (dataSource.equals("json"))
						ds1 = new DataJSONSource(configQuery, mode);
//					}
				}
				String mode = String.valueOf(configInput.get("mode"));
				String dataSource = String.valueOf(configInput.get("dataSource"));
				DataSource ds2 = null;
				if (dataSource.equals("csv"))
					ds2 = new DataCSVSource(configInput, mode);
				else if (dataSource.equals("jdbc"))
					ds2 = new DataJDBCSource(configInput, mode);
				else if (dataSource.equals("es"))
					ds2 = new DataESSource(configInput, mode);
				else if (dataSource.equals("json"))
					ds2 = new DataJSONSource(configInput, mode);

				if (mode.equalsIgnoreCase("standard") || mode.equalsIgnoreCase("fuzzy")) {
					SimJoinRunner runner = new SimJoinRunner(ds1, ds2, configJoin, mode);
					runner.run();
				} else {
					logger.error("Unknown mode");
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			duration = System.nanoTime() - duration;
			System.out.println("Total Join algorithm time: " + duration / 1000000000.0 + " sec.");
			logger.info("Total Join algorithm time: " + duration / 1000000000.0 + " sec.");
		}
	}
}