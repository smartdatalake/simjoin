package eu.smartdatalake.simjoin.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.data.prepared.PreparedFuzzySet;
import eu.smartdatalake.simjoin.data.prepared.PreparedStandardSet;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollectionReader;
import eu.smartdatalake.simjoin.sets.TokenSetCollectionReader;

/**
 * Creates a {@link DataJDBCSource} from the given Database.
 *
 */
public class DataJDBCSource extends DataSource {
	String db, user, pwd, url, charSet, driver;
	String keyCol, tokensCol, weightsCol;

	PoolingDataSource<PoolableConnection> dataSource;

	/**
	 * Creates a {@link DataJDBCSource} from a Database.
	 * 
	 * @param config      The configuration of the input
	 * @param mode        The mode of the input, i.e. Standard or Fuzzy
	 * @return A {@link DataJDBCSource}.
	 */
	public DataJDBCSource(JSONObject config, String mode) {
		super(config, mode, "jdbc");

		db = String.valueOf(config.get("db"));
		user = String.valueOf(config.get("user"));
		pwd = String.valueOf(config.get("pwd"));
		url = String.valueOf(config.get("url"));
		charSet = String.valueOf(config.get("charSet"));
		if (charSet.equals("null") || charSet.equals(""))
			charSet = "utf-8";

		keyCol = String.valueOf(config.get("keyCol"));
		tokensCol = String.valueOf(config.get("tokensCol"));
		weightsCol = String.valueOf(config.get("weightsCol"));
		if (weightsCol.equals("null") || weightsCol.equals(""))
			weightsCol = null;

		if (url.contains("postgres"))
			driver = "org.postgresql.Driver";

		createPool();
	}

	/**
	 * Creates a Pool to connect to the Database.
	 */
	private void createPool() {
		// Specifications for the JDBC connection
		Properties props = new Properties();
		props.put("charSet", charSet);
		props.put("user", user);
		props.put("password", pwd);

		GenericObjectPool<PoolableConnection> connPool = null;
		dataSource = null;

		try {
			Class.forName(driver);

			// A ConnectionFactory that the pool will use to create connections; properties
			// include at least username and password
			DriverManagerConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, props);

			// Implement the pooling functionality
			PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory,
					null);

			// Pool configuration
			GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<PoolableConnection>();
			config.setMaxWaitMillis(500);
			config.setMaxTotal(20);
			config.setMaxIdle(5);
			config.setMinIdle(5);

			connPool = new GenericObjectPool<PoolableConnection>(poolableConnectionFactory, config);
			poolableConnectionFactory.setPool(connPool);
			dataSource = new PoolingDataSource<PoolableConnection>(connPool);
		} catch (ClassNotFoundException e) {
			System.out.println("Cannot create Pool.");
		}
	}

	/**
	 * Creates a {@link Connection} to the Database.
	 * 
	 * @return A {@link Connection}.
	 */
	public Connection getConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			System.out.println("Cannot connect to the JDBC pool.");
			return null;
		}
	}

	/**
	 * Creates a {@link String} statement for querying the Database.
	 * 
	 * @param maxLines Number of Lines to retrieve.
	 * @return A {@link String}.
	 */
	public String prepareStatement(int maxLines) {
		String sql = null;
		if (weightsCol == null)
			sql = String.format("SELECT %s, %s FROM public.\"%s\"", keyCol, tokensCol, db);
		else
			sql = String.format("SELECT %s, %s, %s FROM public.\"%s\"", keyCol, tokensCol, weightsCol, db);
		if (maxLines > 0)
			sql += " LIMIT " + maxLines;
		sql += ";";
		return sql;
	}

	/**
	 * Executes a query to retrieve data from the Database.
	 * 
	 * @param maxLines Number of Lines to retrieve.
	 * @return A {@link ResultSet}.
	 */
	public ResultSet executeStatement(int maxLines) {
		// Get a new connection from the pool
		Connection conn = getConnection();
		if (conn == null) {
			System.out.println("Could not connect to database.");
			return null;
		}

		String sql = prepareStatement(maxLines);

		try {
			Statement stmt = conn.createStatement();
			return stmt.executeQuery(sql);
		} catch (SQLException e) {
			System.out.println("SQL query for data retrieval cannot be executed.");
		}
		return null;
	}

	/**
	 * Whether a weight column has been defined or not.
	 * 
	 * @return A {@link boolean}.
	 */
	public boolean existsWeight() {
		return weightsCol != null;
	}

	/**
	 * Creates a {@link GroupCollection} from the {@link DataJDBCSource}.
	 * 
	 * @param maxLines Number of lines to retrieve from the database.
	 * @return A {@link GroupCollection}.
	 */
	public GroupCollection getData(int maxLines) {
		if (mode.equals("fuzzy"))
			return FuzzySetCollectionReader.fromJDBC(this, maxLines);
		else
			return TokenSetCollectionReader.fromJDBC(this, maxLines);
	}
	
	/**
	 * Prepares the {@link DataSource} by parsing, transforming and creating the index.
	 * 
	 * @param maxLines Number of lines to parse.
	 * @param threshold Threshold for index (only in Standard ThresholdJoin
	 */
	public void prepare(int maxLines, double threshold) {
		if (mode.equals("standard")) {
			prepared = new PreparedStandardSet(TokenSetCollectionReader.fromJDBC(this, maxLines), threshold);	
		} else if (mode.equals("fuzzy")) {
			prepared = new PreparedFuzzySet(FuzzySetCollectionReader.fromJDBC(this, maxLines));
		}
	}
}
