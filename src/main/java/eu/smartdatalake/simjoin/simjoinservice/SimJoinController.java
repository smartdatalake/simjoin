package eu.smartdatalake.simjoin.simjoinservice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import eu.smartdatalake.simjoin.runners.RunFuzzySetSimJoin;
import eu.smartdatalake.simjoin.runners.RunSetSimJoin;
import eu.smartdatalake.simjoin.simjoinservice.models.CatalogInput;
import eu.smartdatalake.simjoin.simjoinservice.models.CatalogOutput;
import eu.smartdatalake.simjoin.simjoinservice.models.JoinInput;
import eu.smartdatalake.simjoin.simjoinservice.models.JoinOutput;
import eu.smartdatalake.simjoin.simjoinservice.models.StatusInput;
import eu.smartdatalake.simjoin.simjoinservice.models.StatusOutput;
import eu.smartdatalake.simjoin.simjoinservice.models.nested.ThreadLog;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
public class SimJoinController {
	private static final Logger logger = LogManager.getLogger(SimJoinController.class);
	private Map<Integer, String> configs;
	private String path = "./dataset_configs/";
	private Map<Integer, ThreadLog> threads;
	private Random rand;

	public SimJoinController() {
		configs = new HashMap<Integer, String>();
		threads = new HashMap<Integer, ThreadLog>();

		File file = new File(path);
		int i = 0;
		for (String s : file.list()) {
			configs.put(++i, s.replace(".json", ""));
		}
		rand = new Random(System.currentTimeMillis());
	}

	@RequestMapping(value = "/simjoin/api/catalog", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ApiOperation("Returns the list of available config files.")
	public ResponseEntity<CatalogOutput> catalog(
			@ApiParam("The client API key") @RequestHeader("api_key") String apiKey,
			@ApiParam("Input to catalog function") @RequestBody CatalogInput input) {

		if (!isValidApiKey(apiKey)) {
			return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
		}
		CatalogOutput result = new CatalogOutput();

		for (Object key : configs.keySet().toArray())
			result.configs.add(key + ". " + configs.get(key));

		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@RequestMapping(value = "/simjoin/api/join", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ApiOperation("Performs a join.")
	public ResponseEntity<JoinOutput> join(@ApiParam("The client API key") @RequestHeader("api_key") String apiKey,
			@ApiParam("Input to search function") @RequestBody JoinInput input) {

		if (!isValidApiKey(apiKey)) {
			return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
		}

		String inputFileConfig = path + configs.get(input.params.params.config_file) + ".json";

		JoinOutput result = null;
		JSONParser jsonParser = new JSONParser();
		try {
			JSONObject config = (JSONObject) jsonParser.parse(new FileReader(inputFileConfig));

			input.params.params.addToConfig(config);
////		String logFile = String.valueOf(config.get("log_file"));
////		System.setProperty("logFilename", logFile);
////		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
////		ctx.reconfigure();

			int id = rand.nextInt();
			config.put("output_file", "output/" + id + ".txt");
//			config.put("log_file", "logs/" + id + ".log");

			Thread thread = new Thread(new Joiner(config));

			ThreadLog lj = new ThreadLog(id, thread, input.params.limit);
			threads.put(id, lj);
			result = new JoinOutput(lj.toString());

			thread.start();

		} catch (FileNotFoundException e) {
			return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
		} catch (IOException e) {
			return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
		} catch (ParseException e) {
			return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@RequestMapping(value = "/simjoin/api/getstatus", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ApiOperation("Returns status of previous execution.")
	public ResponseEntity<StatusOutput> getStatus(
			@ApiParam("The client API key") @RequestHeader("api_key") String apiKey,
			@ApiParam("Input to catalog function") @RequestBody StatusInput input) {

		if (!isValidApiKey(apiKey)) {
			return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
		}

		StatusOutput result = new StatusOutput();
		ThreadLog tl = threads.get(input.id);
		result.status = "" + tl.thread.getState();

		try {
			BufferedReader reader = new BufferedReader(new FileReader(tl.output_file));
			String line;
			long i = 0;
			while ((line = reader.readLine()) != null) {
				if (++i > tl.limit)
					break;
				result.pairs.add(line);
			}
			result.date = new Timestamp(tl.time).toString();
		} catch (IOException e) {
			return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	private static boolean isValidApiKey(String api_key) {

		List<String> validApiKeys = new ArrayList<String>();

		String keysString = System.getProperty("valid_api_keys");
		StringTokenizer strTok = new StringTokenizer(keysString, " ");
		while (strTok.hasMoreTokens()) {
			validApiKeys.add(strTok.nextToken());
		}

		return validApiKeys.contains(api_key) ? true : false;
	}

	public class Joiner implements Runnable {

		JSONObject config;

		public Joiner(JSONObject config) {
			this.config = config;
		}

		@Override
		public void run() {
			String mode = String.valueOf(config.get("mode"));
			if (mode.equalsIgnoreCase("standard")) {
				RunSetSimJoin runner = new RunSetSimJoin();
				runner.execute(config);
			} else if (mode.equalsIgnoreCase("fuzzy")) {
				RunFuzzySetSimJoin runner = new RunFuzzySetSimJoin();
				runner.execute(config);
			} else {
				logger.error("Unknown mode");
			}
		}

	}
}