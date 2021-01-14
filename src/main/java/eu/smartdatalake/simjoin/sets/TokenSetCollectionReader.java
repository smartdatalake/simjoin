package eu.smartdatalake.simjoin.sets;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.data.DataCSVSource;
import eu.smartdatalake.simjoin.data.DataESSource;
import eu.smartdatalake.simjoin.data.DataFileReader;
import eu.smartdatalake.simjoin.data.DataJDBCSource;
import eu.smartdatalake.simjoin.data.DataJSONSource;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
//import org.springframework.data.elasticsearch.core.SearchHit;
import org.elasticsearch.search.SearchHit;

/**
 * Creates a {@link GroupCollection} from the given input.
 *
 */
public class TokenSetCollectionReader {

	private static final Logger logger = LogManager.getLogger(TokenSetCollectionReader.class);

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param ds       {@link DataCSVSource} Source to use for the data.
	 * @param maxLines Number of lines to parse from the file.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<String> fromCSV(DataCSVSource ds, int maxLines) {

		GroupCollection<String> collection = new GroupCollection<String>();
		collection.groups = new ArrayList<Group<String>>();
		int lineCount = 0, errorLines = 0;
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		try {
			DataFileReader dr = new DataFileReader(ds.file);
			String line;
			String[] columns;
			Group<String> set;

			// if the file has header, ignore the first line
			if (ds.header) {
				dr.readLine();
			}

			while ((line = dr.readLine()) != null) {
				if (maxLines > 0 && lineCount + errorLines >= maxLines) {
					break;
				}
				try {
					columns = line.split(ds.columnDelimiter);
					set = new Group<String>();
					set.id = columns[ds.colSetId];

					set.weight = (ds.colWeights == -1) ? 1.0 : Double.parseDouble(columns[ds.colWeights]);
					maxWeight = (set.weight > maxWeight) ? set.weight : maxWeight;
					minWeight = (set.weight < minWeight) ? set.weight : minWeight;

					set.elements = getTokens(columns[ds.colSetTokens], ds.tokenizer, ds.qgram, ds.tokenDelimiter);
					if (set.elements.size() != 0) {
						collection.groups.add(set);
						lineCount++;
					} else {
						errorLines++;
					}
				} catch (Exception e) {
					errorLines++;
				}
			}
			dr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		double elementsPerSet = 0;
		for (Group<String> set : collection.groups) {
			elementsPerSet += set.elements.size();
			if (ds.colWeights != -1)
				set.weight = (set.weight - minWeight) / (maxWeight - minWeight);
		}
		elementsPerSet /= collection.groups.size();

		System.out.println("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.groups.size() + ". Elements per set: " + elementsPerSet);
		logger.info("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: " + errorLines
				+ ". Num of sets: " + collection.groups.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}

	/**
	 * Creates a {@link GroupCollection} from a DataBase.
	 * 
	 * @param ds       {@link DataJDBCSource} Source to use for the data.
	 * @param maxLines Number of lines to retrieve from the Database.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<String> fromJDBC(DataJDBCSource ds, int maxLines) {

		ResultSet resultSet = ds.executeStatement(maxLines);

		GroupCollection<String> collection = new GroupCollection<String>();
		collection.groups = new ArrayList<Group<String>>();
		int lines = 0;
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		try {
			while (resultSet.next()) {
				Group<String> set = new Group<String>();
				set.id = resultSet.getString(1);
				set.weight = (!ds.existsWeight()) ? 1.0 : Double.parseDouble(resultSet.getString(3));
				maxWeight = (set.weight > maxWeight) ? set.weight : maxWeight;
				minWeight = (set.weight < minWeight) ? set.weight : minWeight;

				set.elements = getTokens(resultSet.getString(2), ds.tokenizer, ds.qgram, ds.tokenDelimiter);
				collection.groups.add(set);
				lines++;
			}
			double elementsPerSet = 0;
			for (Group<String> set : collection.groups) {
				elementsPerSet += set.elements.size();
				if (ds.existsWeight())
					set.weight = (set.weight - minWeight) / (maxWeight - minWeight);
			}
			elementsPerSet /= collection.groups.size();

			System.out.println("Finished quering DB. Lines read: " + lines + ". Num of sets: "
					+ collection.groups.size() + ". Elements per set: " + elementsPerSet);
			logger.info("Finished quering DB. Lines read: " + lines + ". Num of sets: " + collection.groups.size()
					+ ". Elements per set: " + elementsPerSet);

			return collection;
		} catch (NumberFormatException | SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Creates a {@link GroupCollection} from an ElasticSearch DB.
	 * 
	 * @param ds       {@link DataJDBCSource} Source to use for the data.
	 * @param maxLines Number of lines to retrieve from the Database.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<String> fromElasticSearch(DataESSource ds, int maxLines) {

		ClientConfiguration clientConfiguration = ClientConfiguration.builder().connectedTo(ds.url).build();
		RestHighLevelClient client = RestClients.create(clientConfiguration).rest();

		SearchRequest searchRequest = new SearchRequest(ds.index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder q = QueryBuilders.matchAllQuery();

		List<String> inputList = new ArrayList<String>();
		inputList.add(ds.colSetTokens);
		if (ds.existsID())
			inputList.add(ds.colSetId);
		if (ds.existsWeight())
			inputList.add(ds.colWeights);

		String[] input = new String[inputList.size()];
		input = inputList.toArray(input);

		searchSourceBuilder.query(q);
		// TODO: Fix size problem
		searchSourceBuilder.size(maxLines);
		searchSourceBuilder.fetchSource(input, new String[] {});
		searchRequest.source(searchSourceBuilder);

		GroupCollection<String> collection = new GroupCollection<String>();
		collection.groups = new ArrayList<Group<String>>();
		int lines = 0;
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		try {
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			SearchHit[] hits = searchResponse.getHits().getHits();
			JSONParser parser = new JSONParser();

			for (SearchHit hit : hits) {
				Group<String> set = new Group<String>();

				JSONObject json = (JSONObject) parser.parse(hit.getSourceAsString());
				if (json.isEmpty())
					continue;

				// ID
				if (!ds.existsID())
					set.id = hit.getId();
				else {
					Object target = json;
					for (String key : ds.colSetId.split("\\."))
						target = ((JSONObject) target).get(key);
					set.id = (String) target;
				}

				// WEIGHT
				if (!ds.existsWeight())
					set.weight = 1.0;
				else {
					Object target = json;
					for (String key : ds.colWeights.split("\\."))
						target = ((JSONObject) target).get(key);
					set.weight = Double.parseDouble((String) target);
				}

				maxWeight = (set.weight > maxWeight) ? set.weight : maxWeight;
				minWeight = (set.weight < minWeight) ? set.weight : minWeight;

				// SET
				Object target = json;
				for (String key : ds.colSetTokens.split("\\."))
					target = ((JSONObject) target).get(key);
				set.elements = getTokens((String) target, ds.tokenizer, ds.qgram, ds.tokenDelimiter);

				collection.groups.add(set);
				lines++;
			}

			double elementsPerSet = 0;
			for (Group<String> set : collection.groups) {
				elementsPerSet += set.elements.size();
				if (ds.existsWeight())
					set.weight = (set.weight - minWeight) / (maxWeight - minWeight);
			}
			elementsPerSet /= collection.groups.size();

			System.out.println("Finished quering DB. Lines read: " + lines + ". Num of sets: "
					+ collection.groups.size() + ". Elements per set: " + elementsPerSet);
			logger.info("Finished quering DB. Lines read: " + lines + ". Num of sets: " + collection.groups.size()
					+ ". Elements per set: " + elementsPerSet);

			return collection;

		} catch (IOException | ParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates a {@link GroupCollection} from a JSON.
	 * 
	 * @param ds       {@link DataCSVSource} Source to use for the data.
	 * @param maxLines Number of lines to parse from the file.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<String> fromJSON(DataJSONSource ds, int maxLines) {

		GroupCollection<String> collection = new GroupCollection<String>();
		collection.groups = new ArrayList<Group<String>>();
		int lineCount = 0, errorLines = 0;
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		for (int i = 0; i < ds.values.size(); i++) {
			JSONObject item = (JSONObject) ds.values.get(i);
			Group<String> set;

			if (maxLines > 0 && lineCount + errorLines >= maxLines) {
				break;
			}
			try {
				set = new Group<String>();
				set.id = (String) item.get("id");
				set.weight = (!ds.existsWeight()) ? 1.0 : (double) item.get("weight");
				maxWeight = (set.weight > maxWeight) ? set.weight : maxWeight;
				minWeight = (set.weight < minWeight) ? set.weight : minWeight;

				set.elements = getTokens((String) item.get("set"), ds.tokenizer, ds.qgram, ds.tokenDelimiter);
				if (set.elements.size() != 0) {
					collection.groups.add(set);
					lineCount++;
				} else {
					errorLines++;
				}
			} catch (Exception e) {
				e.printStackTrace();
				errorLines++;
			}
		}

		double elementsPerSet = 0;
		for (Group<String> set : collection.groups) {
			elementsPerSet += set.elements.size();
			if (ds.existsWeight())
				set.weight = (set.weight - minWeight) / (maxWeight - minWeight);
		}
		elementsPerSet /= collection.groups.size();

		System.out.println("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.groups.size() + ". Elements per set: " + elementsPerSet);
		logger.info("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: " + errorLines
				+ ". Num of sets: " + collection.groups.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}

	private static ArrayList<String> getTokens(String line, String tokenizer, int qgram, String tokenDelimiter) {

		TObjectIntMap<String> tokens = new TObjectIntHashMap<String>();
		if (tokenizer.equals("qgram")) {
			String token = line;
			for (int i = 0; i <= token.length() - qgram; i++) {
				tokens.adjustOrPutValue(token.substring(i, i + qgram), 1, 0);
			}
		} else {
			for (String tok : Arrays.asList(line.split(tokenDelimiter))) {
				if (!tok.equals(""))
					tokens.adjustOrPutValue(tok, 1, 0);
			}
		}

		ArrayList<String> tokens2 = new ArrayList<String>();
		for (String key : tokens.keySet()) {
			for (int val = 0; val <= tokens.get(key); val++) {
				tokens2.add(key + "@" + val);
			}
		}

		return tokens2;
	}
}