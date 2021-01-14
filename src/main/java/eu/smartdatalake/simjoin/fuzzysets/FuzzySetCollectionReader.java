package eu.smartdatalake.simjoin.fuzzysets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.data.DataCSVSource;
import eu.smartdatalake.simjoin.data.DataESSource;
import eu.smartdatalake.simjoin.data.DataFileReader;
import eu.smartdatalake.simjoin.data.DataJDBCSource;
import eu.smartdatalake.simjoin.data.DataJSONSource;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.sql.ResultSet;

/**
 * Creates a {@link FuzzySetCollectionReader} from the given input.
 *
 */
public class FuzzySetCollectionReader {

	private static final Logger logger = LogManager.getLogger(FuzzySetCollectionReader.class);

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param ds       {@link DataCSVSource} Source to use for the data.
	 * @param maxLines Number of lines to parse from the file.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<ArrayList<String>> fromCSV(DataCSVSource ds, int maxLines) {

		Map<String, ArrayList<ArrayList<String>>> collection = new LinkedHashMap<String, ArrayList<ArrayList<String>>>();
		TObjectDoubleMap<String> weights = new TObjectDoubleHashMap<String>();

		double tokensPerElement = 0;
		DataFileReader dr;
		int lines = 0;
		int errorLines = 0;

		try {
			dr = new DataFileReader(ds.file);

			String line, set;
			String[] columns;
			ArrayList<ArrayList<String>> elements;

			// if the file has header, ignore the first line
			if (ds.header) {
				dr.readLine();
			}

			while ((line = dr.readLine()) != null) {
				try {
					columns = line.split(ds.columnDelimiter);

					set = columns[ds.colSetId];

					ArrayList<String> tokens = getTokens(columns[ds.colSetTokens], ds.tokenizer, ds.qgram,
							ds.tokenDelimiter);

					if (tokens.size() != 0) {
						tokensPerElement += tokens.size();
						elements = collection.get(set);
						if (elements == null) {
							elements = new ArrayList<ArrayList<String>>();
						}

						elements.add(tokens);
						collection.put(set, elements);

						double weight = (ds.colWeights == -1) ? 1.0 : Double.parseDouble(columns[ds.colWeights]);
						weights.adjustOrPutValue(set, weight, weight);

						lines++;
					} else {
						errorLines++;
					}
				} catch (Exception e) {
					errorLines++;
				}
				if (maxLines > -1 && lines + errorLines >= maxLines) {
					break;
				}
			}

			dr.close();

			double elementsPerSet = lines / collection.size();
			tokensPerElement /= lines;

			GroupCollection<ArrayList<String>> groupCollection = createCollection(collection, weights,
					ds.colWeights != -1);

			System.out.println("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: "
					+ errorLines + ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet
					+ ". Tokens per Element: " + tokensPerElement);
			logger.info("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: " + errorLines
					+ ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet
					+ ". Tokens per Element: " + tokensPerElement);

			return groupCollection;
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Creates a {@link GroupCollection} from a CSV file.
	 * 
	 * @param ds       {@link DataJDBCSource} Source to use for the data.
	 * @param maxLines Number of lines to retrieve from the Database.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<ArrayList<String>> fromJDBC(DataJDBCSource ds, int maxLines) {
		try {
			ResultSet resultSet = ds.executeStatement(maxLines);

			Map<String, ArrayList<ArrayList<String>>> collection = new LinkedHashMap<String, ArrayList<ArrayList<String>>>();
			TObjectDoubleMap<String> weights = new TObjectDoubleHashMap<String>();
			double tokensPerElement = 0;

			ArrayList<ArrayList<String>> elements;
			int lines = 0;
			while (resultSet.next()) {
				String set = resultSet.getString(1);

				ArrayList<String> tokens = getTokens(resultSet.getString(2), ds.tokenizer, ds.qgram, ds.tokenDelimiter);

				tokensPerElement += tokens.size();
				elements = collection.get(set);
				if (elements == null) {
					elements = new ArrayList<ArrayList<String>>();
				}
				elements.add(tokens);
				collection.put(set, elements);

				double weight = (!ds.existsWeight()) ? 1.0 : Double.parseDouble(resultSet.getString(3));
				weights.adjustOrPutValue(set, weight, weight);
				lines++;
			}

			double elementsPerSet = lines / collection.size();
			tokensPerElement /= lines;

			GroupCollection<ArrayList<String>> groupCollection = createCollection(collection, weights,
					ds.existsWeight());

			System.out.println("Finished quering DB. Lines read: " + lines + ". Num of sets: " + collection.size()
					+ ". Elements per set: " + elementsPerSet + ". Tokens per Element: " + tokensPerElement);
			logger.info("Finished quering DB. Lines read: " + lines + ". Num of sets: " + collection.size()
					+ ". Elements per set: " + elementsPerSet + ". Tokens per Element: " + tokensPerElement);
			return groupCollection;
		} catch (Exception e) {
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
	public static GroupCollection<ArrayList<String>> fromElasticSearch(DataESSource ds, int maxLines) {

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

		Map<String, ArrayList<ArrayList<String>>> collection = new LinkedHashMap<String, ArrayList<ArrayList<String>>>();
		TObjectDoubleMap<String> weights = new TObjectDoubleHashMap<String>();
		double tokensPerElement = 0;

		ArrayList<ArrayList<String>> elements;
		int lines = 0;

		try {
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			SearchHit[] hits = searchResponse.getHits().getHits();
			JSONParser parser = new JSONParser();

			for (SearchHit hit : hits) {

				JSONObject json = (JSONObject) parser.parse(hit.getSourceAsString());
				if (json.isEmpty())
					continue;

				String id = null;
				// ID
				if (!ds.existsID())
					id = hit.getId();
				else {
					Object target = json;
					for (String key : ds.colSetId.split("\\."))
						target = ((JSONObject) target).get(key);
					id = (String) target;
				}

				// SET
				Object target = json;
				for (String key : ds.colSetTokens.split("\\."))
					target = ((JSONObject) target).get(key);
				ArrayList<String> tokens = getTokens((String) target, ds.tokenizer, ds.qgram, ds.tokenDelimiter);

				tokensPerElement += tokens.size();
				elements = collection.get(id);
				if (elements == null) {
					elements = new ArrayList<ArrayList<String>>();
				}
				elements.add(tokens);
				collection.put(id, elements);

				// WEIGHT
				double weight = 1.0;
				if (ds.existsWeight()) {
					target = json;
					for (String key : ds.colWeights.split("\\."))
						target = ((JSONObject) target).get(key);
					weight = Double.parseDouble((String) target);
				}

				weights.adjustOrPutValue(id, weight, weight);
				lines++;
			}

			double elementsPerSet = lines / collection.size();
			tokensPerElement /= lines;

			GroupCollection<ArrayList<String>> groupCollection = createCollection(collection, weights,
					ds.existsWeight());

			System.out.println("Finished quering DB. Lines read: " + lines + ". Num of sets: " + collection.size()
					+ ". Elements per set: " + elementsPerSet + ". Tokens per Element: " + tokensPerElement);
			logger.info("Finished quering DB. Lines read: " + lines + ". Num of sets: " + collection.size()
					+ ". Elements per set: " + elementsPerSet + ". Tokens per Element: " + tokensPerElement);
			return groupCollection;
		} catch (IOException | ParseException e) {
			return null;
		}
	}

	/**
	 * Creates a {@link GroupCollection} from a JSON.
	 * 
	 * @param ds       {@link DataJDBCSource} Source to use for the data.
	 * @param maxLines Number of lines to retrieve from the Database.
	 * @return A {@link GroupCollection}.
	 */
	public static GroupCollection<ArrayList<String>> fromJSON(DataJSONSource ds, int maxLines) {
		Map<String, ArrayList<ArrayList<String>>> collection = new LinkedHashMap<String, ArrayList<ArrayList<String>>>();
		TObjectDoubleMap<String> weights = new TObjectDoubleHashMap<String>();

		double tokensPerElement = 0;
		int lines = 0;
		int errorLines = 0;

		for (int i = 0; i < ds.values.size(); i++) {
			JSONObject item = (JSONObject) ds.values.get(i);
			String id = (String) item.get("id");

			ArrayList<ArrayList<String>> elements;

			ArrayList<String> tokens = getTokens((String) item.get("set"), ds.tokenizer, ds.qgram, ds.tokenDelimiter);

			if (tokens.size() != 0) {
				tokensPerElement += tokens.size();
				elements = collection.get(id);
				if (elements == null) {
					elements = new ArrayList<ArrayList<String>>();
				}

				elements.add(tokens);
				collection.put(id, elements);

				double weight = (!ds.existsWeight()) ? 1.0 : (double) item.get("weight");
				weights.adjustOrPutValue(id, weight, weight);

				lines++;
			} else {
				errorLines++;
			}
			if (maxLines > -1 && lines + errorLines >= maxLines) {
				break;
			}

		}
		double elementsPerSet = lines / collection.size();
		tokensPerElement /= lines;

		GroupCollection<ArrayList<String>> groupCollection = createCollection(collection, weights, ds.existsWeight());

		System.out.println("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet
				+ ". Tokens per Element: " + tokensPerElement);
		logger.info("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: " + errorLines
				+ ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet
				+ ". Tokens per Element: " + tokensPerElement);

		return groupCollection;
	}

	private static GroupCollection<ArrayList<String>> createCollection(
			Map<String, ArrayList<ArrayList<String>>> collection, TObjectDoubleMap<String> weights,
			boolean colWeights) {
		double minWeight = Double.MAX_VALUE, maxWeight = Double.MIN_VALUE;

		for (String set : collection.keySet()) {
			double weight = weights.get(set);
			if (weight >= maxWeight)
				maxWeight = weight;
			if (weight <= minWeight)
				minWeight = weight;
		}

		// transform to group collection
		GroupCollection<ArrayList<String>> groupCollection = new GroupCollection<ArrayList<String>>();
		groupCollection.groups = new ArrayList<Group<ArrayList<String>>>();
		Group<ArrayList<String>> group;
		for (String key : collection.keySet()) {
			group = new Group<ArrayList<String>>();
			group.id = key;
			group.elements = collection.get(key);
			if (colWeights) {
				group.weight = weights.get(key);
				group.weight = (weights.get(key) - minWeight) / (maxWeight - minWeight);
			} else
				group.weight = 1.0;

			groupCollection.groups.add(group);
		}
		return groupCollection;
	}

	private static ArrayList<String> getTokens(String line, String tokenizer, int qgram, String tokenDelimiter) {
		TObjectIntMap<String> tokens = new TObjectIntHashMap<String>();

		if (tokenizer.equals("qgram")) {
			for (int i = 0; i <= line.length() - qgram; i++) {
				tokens.adjustOrPutValue(line.substring(i, i + qgram), 1, 0);
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