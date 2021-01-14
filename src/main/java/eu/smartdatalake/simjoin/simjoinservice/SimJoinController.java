package eu.smartdatalake.simjoin.simjoinservice;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import eu.smartdatalake.simjoin.data.DataCSVSource;
import eu.smartdatalake.simjoin.data.DataESSource;
import eu.smartdatalake.simjoin.data.DataJDBCSource;
import eu.smartdatalake.simjoin.data.DataJSONSource;
import eu.smartdatalake.simjoin.data.DataSource;
import eu.smartdatalake.simjoin.runners.SimJoinRunner;
import eu.smartdatalake.simjoin.simjoinservice.models.AddSourceInput;
import eu.smartdatalake.simjoin.simjoinservice.models.CatalogOutput;
import eu.smartdatalake.simjoin.simjoinservice.models.GeneralInput;
import eu.smartdatalake.simjoin.simjoinservice.models.GeneralOutput;
import eu.smartdatalake.simjoin.simjoinservice.models.JoinInput;
import eu.smartdatalake.simjoin.simjoinservice.models.StatusOutput;
import eu.smartdatalake.simjoin.simjoinservice.models.nested.ThreadLog;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
public class SimJoinController {
	private static final Logger logger = LogManager.getLogger(SimJoinController.class);

	private Map<String, User> users;
	private long timeout;

	public SimJoinController() {
		try {
			timeout = Long.parseLong(System.getProperty("timeout"));
			timeout = timeout * 1000000000;
		} catch (Exception e) {
			timeout = -1;
		}

		users = new HashMap<String, User>();
	}

	@RequestMapping(value = "/simjoin/api/addsource", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ApiOperation("Add a new DataSource")
	public ResponseEntity<GeneralOutput[]> addSource(
			@ApiParam("Input to addSource function") @RequestBody AddSourceInput[] inputs) {
		String userID = String.format("USR_%d", System.currentTimeMillis());

		users.put(userID, new User(userID));

		User u = users.get(userID);

		GeneralOutput[] results = new GeneralOutput[inputs.length];

		for (int i=0; i< inputs.length; i++) {
			String id = String.format("%d_%d", System.currentTimeMillis(), i);
			String msg = addSource(inputs[i], id, u);
			results[i] = new GeneralOutput(msg, id);
		}

		MultiValueMap<String, String> header = new LinkedMultiValueMap<String, String>();
		header.add("id", userID);
		return new ResponseEntity<GeneralOutput[]>(results, header, HttpStatus.OK);
	}

	@RequestMapping(value = "/simjoin/api/appendsource", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ApiOperation("Append a new DataSource")
	public ResponseEntity<GeneralOutput[]> appendSource(
			@ApiParam("The client API key") @RequestHeader("api_key") String apiKey,
			@ApiParam("Input to addSource function") @RequestBody AddSourceInput[] inputs) {

		if (!users.containsKey(apiKey)) {
			return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
		}

		User u = users.get(apiKey);

		GeneralOutput[] results = new GeneralOutput[inputs.length];

		for (int i=0; i< inputs.length; i++) {
			String id = String.format("%d_%d", System.currentTimeMillis(), i);
			String msg = addSource(inputs[i], id, u);
			results[i] = new GeneralOutput(msg, id);
		}
		MultiValueMap<String, String> header = new LinkedMultiValueMap<String, String>();
		header.add("id", apiKey);
		return new ResponseEntity<GeneralOutput[]>(results, header, HttpStatus.OK);
	}

	@RequestMapping(value = "/simjoin/api/removesource", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ApiOperation("Remove a DataSource")
	public ResponseEntity<GeneralOutput> removeSource(
			@ApiParam("The client API key") @RequestHeader("api_key") String apiKey,
			@ApiParam("Input to removeSource function") @RequestBody GeneralInput input) {

		if (!users.containsKey(apiKey)) {
			return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
		}

		User u = users.get(apiKey);

		u.removeDataSource(input.id);

		String msg = String.format("DataSource with ID %s was succesfully removed", input.id);
		GeneralOutput result = new GeneralOutput(msg, input.id);
		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@RequestMapping(value = "/simjoin/api/catalog", method = RequestMethod.POST, produces = "application/json")
	@ApiOperation("Returns the list of available config files.")
	public ResponseEntity<CatalogOutput> catalog(
			@ApiParam("The client API key") @RequestHeader("api_key") String apiKey) {

		if (!users.containsKey(apiKey)) {
			return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
		}

		User u = users.get(apiKey);

		CatalogOutput result = new CatalogOutput(u.getDataSources());

		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@RequestMapping(value = "/simjoin/api/startjoin", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ApiOperation("Starts a join.")
	public ResponseEntity<GeneralOutput> start_join(
			@ApiParam("The client API key") @RequestHeader("api_key") String apiKey,
			@ApiParam("An extra client API key (Optional) ") @RequestHeader(value = "api_key2", required = false) String apiKey2,
			@ApiParam("Input to search function") @RequestBody JoinInput input) {

		if (!users.containsKey(apiKey)) {
			return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
		}

		User u1 = users.get(apiKey);
		DataSource ds1 = u1.getDataSource(input.params.query_dataSource);

		DataSource ds2 = null;
		if (apiKey2 != null) {
			if (!users.containsKey(apiKey2)) {
				return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
			}
			User u2 = users.get(apiKey2);
			ds2 = u2.getDataSource(input.params.input_dataSource);
		} else {
			ds2 = u1.getDataSource(input.params.input_dataSource);
		}

		JSONObject config = input.toJSON();
////	String logFile = String.valueOf(config.get("log_file"));
////	System.setProperty("logFilename", logFile);
////	LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
////	ctx.reconfigure();

		String id = String.valueOf(System.currentTimeMillis());
//		config.put("log_file", "logs/" + id + ".log");
		config.put("output_file", "out/" + id + ".txt");
		config.put("mode", ds2.mode);

		if (!ds2.mode.equals("standard") && !ds2.mode.equals("fuzzy")) {
			return new ResponseEntity<>(null, HttpStatus.BAD_REQUEST);
		}

		SimJoinRunner runner = new SimJoinRunner(ds1, ds2, config, ds2.mode, timeout);

		ThreadLog lj = new ThreadLog(id, runner, input.limit);
		u1.addThreadLog(id, lj);
		GeneralOutput result = new GeneralOutput(lj.toString(), id);
		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@RequestMapping(value = "/simjoin/api/getstatus", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ApiOperation("Returns status of previous execution.")
	public ResponseEntity<StatusOutput> getStatus(
			@ApiParam("The client API key") @RequestHeader("api_key") String apiKey,
			@ApiParam("Input to catalog function") @RequestBody GeneralInput input) {

		if (!users.containsKey(apiKey)) {
			return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
		}

		User u = users.get(apiKey);

		StatusOutput result = new StatusOutput();
		ThreadLog tl = u.getThreadLog(input.id);
		result.status = "" + tl.thread.getState();

		try {
			BufferedReader reader = new BufferedReader(new FileReader(tl.output_file));
			String line;
			long i = 0;
			while ((line = reader.readLine()) != null) {
				if (++i > tl.limit) {
					tl.thread.interrupt();
					break;
				}
				result.pairs.add(line);
			}
			result.date = new Timestamp(tl.time).toString();
			reader.close();
		} catch (IOException e) {
			return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity<>(result, HttpStatus.OK);
	}
	
	private String addSource(AddSourceInput input, String id, User u) {
		JSONObject config = input.toConfig();
		DataSource ds = null;
		if (input.type.equals("jdbc"))
			ds = new DataJDBCSource(config, input.mode);
		else if (input.type.equals("csv"))
			ds = new DataCSVSource(config, input.mode);
		else if (input.type.equals("es"))
			ds = new DataESSource(config, input.mode);
		else if (input.type.equals("json"))
			ds = new DataJSONSource(config, input.mode);
		if (input.prepare != null) {
			ds.prepare(input.prepare.max_lines, input.prepare.threshold);
		}
		u.addDataSource(id, input.name, ds);
		String msg = String.format("DataSource %s was succesfully added with ID: %s", input.name, id);
		return msg;
	}
}