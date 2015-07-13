package fs.orientdb;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
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
        // ensure the existence of the database requested
        if (this.config.getDatabaseType().equals(OrientConfiguration.DATABASE_REMOTE)) {
            OServerAdmin remoteServer = new OServerAdmin(this.config.getDatabaseType() + ":" + this.config.getUrl() + "/" + database);
            if (this.config.getUsername()!=null && this.config.getPassword()!=null) {
                remoteServer.connect(this.config.getUsername(), this.config.getPassword());
            }
            // If the given schema does not exist, create it
            if (!remoteServer.existsDatabase("plocal")) {
                remoteServer.createDatabase("graph", "plocal");
            }
            remoteServer.close();
        } else {
            // Create connection with database
        	ODatabaseDocumentTx memoryServer = new ODatabaseDocumentTx(this.config.getDatabaseType() + ":" + this.config.getUrl() + "/" + database);
            if (!memoryServer.exists()) {
                memoryServer.create();
            }
            memoryServer.close();
        }

        // Connect to database and set conflict strategy
        if (strategy!=null) {
	        ODatabase factory = getOFactory(database);
	        factory.setConflictStrategy(strategy);
        }
        
        // create factory for the database
        return new DB(new OrientGraphFactory(this.config.getDatabaseType() + ":" +
                this.config.getUrl() + "/" + database)
                .setupPool(this.config.getMinPool(), this.config.getMaxPool()), false);
    }
    
    /**
     * Drops a given database
     * @throws IOException
     */
    public void dropDatabase(String database) throws IOException {
    	// ensure the existence of the database requested
        if (this.config.getDatabaseType().equals(OrientConfiguration.DATABASE_REMOTE)) {
            OServerAdmin remoteServer = new OServerAdmin(this.config.getDatabaseType() + ":" + this.config.getUrl() + "/" + database);
            if (this.config.getUsername()!=null && this.config.getPassword()!=null) {
                remoteServer.connect(this.config.getUsername(), this.config.getPassword());
            }
            // If the given schema does not exist, create it
            if (remoteServer.existsDatabase("plocal")) {
                remoteServer.dropDatabase(this.config.getUrl());
            }
            remoteServer.close();
        } else {
            // Create connection with database
        	ODatabaseDocumentTx memoryServer = new ODatabaseDocumentTx(this.config.getDatabaseType() + ":" + this.config.getUrl() + "/" + database);
            if (memoryServer.exists()) {
                memoryServer.getStorage().delete();
            }
            memoryServer.close();
        }
    }
    
    /**
     * Returns if the specified database exists on the current server instance
     * @param database
     * @return
     * @throws IOException
     */
    public boolean existsDatabase(String database) throws IOException {
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
    	OServerAdmin remoteServer = new OServerAdmin(this.config.getDatabaseType() + ":" + this.config.getUrl());
        if (this.config.getUsername()!=null && this.config.getPassword()!=null) {
            remoteServer.connect(this.config.getUsername(), this.config.getPassword());
        }
        return remoteServer;
    }
    
    /**
     * Returns an OrientGraphFactory instance for the given database
     * @param database
     * @return
     */
    private OrientGraphFactory buildFactory(String database) {
    	return new OrientGraphFactory(this.config.getDatabaseType() + ":" +
                this.config.getUrl() + "/" + database)
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
