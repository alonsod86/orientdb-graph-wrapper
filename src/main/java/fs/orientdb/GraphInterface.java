package fs.orientdb;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import fs.orientdb.constants.CONFLICT_STRATEGY;


/**
 * Connection Pool for OrientDB graph database
 * Created by dgutierrez on 23/5/15.
 */
public class GraphInterface {
	static Logger log = LoggerFactory.getLogger(GraphInterface.class.getSimpleName());

	/**  the default, throw an exception when versions are different */
	public static final String CONFLIC_STRATEGY_VERSION = "version";
	/** in case the version is different checks if the content is changed, otherwise use the highest version and avoid throwing exception */
	public static final String CONFLIC_STRATEGY_CONTENT = "content";
	/**  merges the changes */
	public static final String CONFLIC_STRATEGY_AUTOMERGE = "automerge";

	// Instance configuration for graph database
	private OrientConfiguration config = new OrientConfiguration();

	// Contains last active url to an Orientdb instance
	private String activeConnectionUrl;

	/**
	 * Instantiate an OrientDB graph database using configuration class
	 * @param config
	 */
	public GraphInterface(OrientConfiguration config) {
		this.config = config;
	}

	/**
	 * Instantiate an OrientDB graph database using remote connection by default
	 * @param url
	 * @param schema
	 */
	public GraphInterface(String url) {
		this.config = new OrientConfiguration(url);
	}

	/**
	 * Instantiate an authenticated OrientDB graph database using remote connection by default
	 * @param url
	 * @param schema
	 */
	public GraphInterface(String url, String user, String password) {
		this.config = new OrientConfiguration(url, user, password);
	}

	/**
	 * Instantiate an authenticated OrientDB graph database using remote connection by default
	 * @param url
	 * @param schema
	 */
	public GraphInterface(String url, Integer poolMin, Integer poolMax, String user, String password) {
		this.config = new OrientConfiguration(url, poolMin, poolMax, user, password);
	}

	/**
	 * Instantiate an authenticated OrientDB graph database using remote connection by default
	 * @param url
	 * @param schema
	 */
	public GraphInterface(String url, Integer poolMin, Integer poolMax, String user, String password, String databaseType) {
		this.config = new OrientConfiguration(url, poolMin, poolMax, user, password, databaseType);
	}

	/**
	 *  Crates a new database if it does not exist, returning a non transactional instance of it
	 * @param database
	 * @return
	 * @throws IOException
	 */
	public DB createDatabase(String database) throws IOException {
		return createDatabase(database, null);
	}

	/**
	 * Crates a new database providing the conflict strategy that will be used by default, returning a non transactional instance of it
	 */
	public DB createDatabase(String database, CONFLICT_STRATEGY strategy) throws IOException {
		String finalURL;
		// ensure the existence of the database requested
		if (this.config.getDatabaseType().equals(OrientConfiguration.DATABASE_REMOTE)) {
			OServerAdmin remoteServer = getOServer();
			// If the given schema does not exist, create it
			if (!remoteServer.existsDatabase("plocal")) {
				remoteServer.createDatabase("graph", "plocal");
			}
			remoteServer.close();
			finalURL = this.activeConnectionUrl;
		} else {
			// Create connection with database
			ODatabaseDocumentTx memoryServer = new ODatabaseDocumentTx(this.config.getDatabaseType() + ":" + this.config.getUrls()[0] + "/" + database);
			if (!memoryServer.exists()) {
				memoryServer.create();
			}
			memoryServer.close();
			finalURL = this.config.getUrls()[0];
		}

		// Connect to database and set conflict strategy
		if (strategy!=null) {
			ODatabase factory = getOFactory(database);
			factory.setConflictStrategy(strategy);
		}

		// create factory for the database
		return new DB(new OrientGraphFactory(this.config.getDatabaseType() + ":" +
				finalURL + "/" + database)
		.setupPool(this.config.getMinPool(), this.config.getMaxPool()), false);
	}

	/**
	 * Drops a given database
	 * @throws IOException
	 */
	public void dropDatabase(String database) throws IOException {
		if (this.activeConnectionUrl==null) this.activeConnectionUrl = getActiveServerUrl(this.config);

		// ensure the existence of the database requested
		if (this.config.getDatabaseType().equals(OrientConfiguration.DATABASE_REMOTE)) {
			OServerAdmin remoteServer = getOServer();
			// If the given schema does not exist, create it
			if (remoteServer.existsDatabase("plocal")) {
				remoteServer.dropDatabase(this.activeConnectionUrl);
			}
			remoteServer.close();
		} else {
			// Create connection with database (plocal or memory only contains one url to connect to)
			@SuppressWarnings("resource")
			ODatabaseDocumentTx memoryServer = new ODatabaseDocumentTx(this.config.getDatabaseType() + ":" + this.config.getUrls()[0] + "/" + database);
			memoryServer.activateOnCurrentThread();
			memoryServer.open(this.config.getUsername(), this.config.getPassword());
			if (memoryServer.exists()) {
				memoryServer.drop();
			}
		}
	}

	/**
     * Returns if the specified database exists on the current server instance
     * @param database
     * @return
     * @throws IOException
     */
    public boolean existsDatabase(String database) throws IOException {
    	if (this.activeConnectionUrl==null) this.activeConnectionUrl = getActiveServerUrl(this.config);
    	// ensure the existence of the database requested
        if (this.config.getDatabaseType().equals(OrientConfiguration.DATABASE_REMOTE)) {
            OServerAdmin remoteServer = new OServerAdmin(this.config.getDatabaseType() + ":" + this.config.getUrl() + "/" + database);
            if (this.config.getUsername()!=null && this.config.getPassword()!=null) {
                remoteServer.connect(this.config.getUsername(), this.config.getPassword());
            }
            // If the given schema does not exist, create it
            boolean exists = remoteServer.existsDatabase("plocal");
            remoteServer.close();
            return exists;
        } else {
            // Create connection with database
        	ODatabaseDocumentTx memoryServer = new ODatabaseDocumentTx(this.config.getDatabaseType() + ":" + this.config.getUrl() + "/" + database);
            boolean exists = memoryServer.exists();
            memoryServer.close();
            return exists;
        }
    }

	/**
	 * Returns an instance to a remote or local instance of OrientDB admin server
	 * @return
	 * @throws IOException
	 */
	public OServerAdmin getOServer() throws IOException {
		if (this.activeConnectionUrl==null) this.activeConnectionUrl = getActiveServerUrl(this.config);

		OServerAdmin remoteServer = new OServerAdmin(this.config.getDatabaseType() + ":" + this.activeConnectionUrl);
		if (this.config.getUsername()!=null && this.config.getPassword()!=null) {
			try {
				// try to connect. If failure check other urls in cluster
				remoteServer.connect(this.config.getUsername(), this.config.getPassword());
			} catch (OIOException oe) {
				// retry connection just in case one shard is down
				String url = getActiveServerUrl(this.config);
				remoteServer = new OServerAdmin(this.config.getDatabaseType() + ":" + url);
				try {
					remoteServer.connect(this.config.getUsername(), this.config.getPassword());
					this.activeConnectionUrl = url;
				} catch (Exception e) {
					log.error("Could not get active connection after {} retries. Aborting", this.config.getUrls().length);
				}
			}
		}

		return remoteServer;
	}

	/**
	 * Iterates over every connection available to find an active one
	 * @return
	 * @throws IOException
	 */
	private String getActiveServerUrl(OrientConfiguration config) {
		String[] urls = config.getUrls();
		for (String url : urls) {
			OServerAdmin remoteServer = null;
			try {
				remoteServer = new OServerAdmin(config.getDatabaseType() + ":" + url);
				// if connection has user and password check connection
				if (config.getDatabaseType().equals(OrientConfiguration.DATABASE_REMOTE) && config.getUsername()!=null && config.getPassword()!=null) {
					// prove connection. If valid, return ip address
					remoteServer.connect(config.getUsername(), config.getPassword());
					remoteServer.close();
					return url;
				// if no user and password return first instance
				} else {
					return url;
				}
			} catch (OStorageException oe) {
				if (urls.length==1) {
					log.warn("Connection {} is not active", config.getDatabaseType() + ":" + url);
				} else {
					log.warn("Connection {} is not active. Retrying with another ip...", config.getDatabaseType() + ":" + url);
				}
			} catch (Exception e) {
				log.error("Could not estabilish connection with server {}. Reason {}", config.getDatabaseType() + ":" + url, e.getMessage());
			}
		}

		// no active connections found
		return null;
	}

	/**
	 * Returns an OrientGraphFactory instance for the given database
	 * @param database
	 * @return
	 */
	private OrientGraphFactory buildFactory(String database) {
		if (this.activeConnectionUrl==null) this.activeConnectionUrl = getActiveServerUrl(this.config);

		return new OrientGraphFactory(this.config.getDatabaseType() + ":" +
				this.activeConnectionUrl + "/" + database)
		.setupPool(this.config.getMinPool(), this.config.getMaxPool());
	}

	/**
	 * Returns an instance to the database factory
	 * @param database
	 * @return
	 */
	public ODatabase getOFactory(String database) {
		return new ODatabase(buildFactory(database), this);
	}

	/**
	 * Closes a database and returns it to the pool
	 * @param db
	 */
	public void closeDB(OrientBaseGraph db) {
		if (db!=null) {
			try {
				db.shutdown();
			} catch (Exception e) {
				log.error("Error closingDB", e);
			}
		}
	}
}

