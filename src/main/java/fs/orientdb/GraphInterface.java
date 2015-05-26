package fs.orientdb;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import java.io.IOException;

/**
 * Connection Pool for OrientDB graph database
 * Created by dgutierrez on 23/5/15.
 */
public class GraphInterface {
    // Instance configuration for graph database
    private OrientConfiguration config = new OrientConfiguration();
    // Factory builder for database
    private OrientGraphFactory factory;

    // Instance of OServer when connection is local/remote
    private OServerAdmin remoteServer;

    // Instance of OServer when in memory
    private ODatabaseDocumentTx memoryServer;

    /**
     * Instantiate an OrientDB graph database using configuration class
     * @param config
     */
    public GraphInterface(OrientConfiguration config) {
        this.config = config;
    }

    /**
     * Instantiate an OrientDB graph database using remote connection by default
     * @param database
     * @param schema
     */
    public GraphInterface(String database, String schema) {
        this.config = new OrientConfiguration(database, schema);
    }

    /**
     * Instantiate an authenticated OrientDB graph database using remote connection by default
     * @param database
     * @param schema
     */
    public GraphInterface(String database, String schema, String user, String password) {
        this.config = new OrientConfiguration(database, schema, user, password);
    }
    
    /**
     * Instantiate an authenticated OrientDB graph database using remote connection by default
     * @param database
     * @param schema
     */
    public GraphInterface(String database, String schema, Integer poolMin, Integer poolMax, String user, String password) {
        this.config = new OrientConfiguration(database, schema, poolMin, poolMax, user, password);
    }
    
    /**
     * Instantiate an authenticated OrientDB graph database using remote connection by default
     * @param database
     * @param schema
     */
    public GraphInterface(String database, String schema, Integer poolMin, Integer poolMax, String user, String password, String databaseType) {
    	this.config = new OrientConfiguration(database, schema, poolMin, poolMax, user, password, databaseType);
    }

    /**
     * Instantiate an OrientDB graph database using the type of database given in constructor
     * @see OrientConfiguration
     * @param databaseType
     * @param database
     * @param schema
     */
    public GraphInterface(String databaseType, String database, String schema) {
        this.config = new OrientConfiguration(database, schema);
        this.config.setDatabaseType(databaseType);
    }

    /**
     * Authenticates the current instance using the given credentials
     * @param username
     * @param password
     */
    public void setAuthentication(String username, String password) {
        this.config.setUsername(username);
        this.config.setPassword(password);
    }

    /**
     * Initialise graph factory with the given configuration
     */
    private void initGraphFactory() throws IOException {
        // ensure the existence of the database requested
        if (this.config.getDatabaseType().equals(OrientConfiguration.DATABASE_REMOTE)) {
            remoteServer = new OServerAdmin(this.config.getDatabaseType() + ":" + this.config.getDatabase() + "/" + this.config.getSchema());
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
            memoryServer = new ODatabaseDocumentTx(this.config.getDatabaseType() + ":" + this.config.getDatabase() + "/" + this.config.getSchema());
            if (!memoryServer.exists()) {
                memoryServer.create();
            }
            memoryServer.close();
        }

        // create factory for the database
        factory = new OrientGraphFactory(this.config.getDatabaseType() + ":" +
                this.config.getDatabase() + "/" + this.config.getSchema())
                .setupPool(this.config.getMinPool(), this.config.getMaxPool());

    }

    /**
     * Returns a non transactional instance of the database
     * @return
     */
    public DB getDB() throws IOException {
        return getDB(false);
    }

    /**
     * Returns an instance to de database specifying if transactional or not
     * @param transactional
     * @return
     */
    public DB getDB(boolean transactional) throws IOException {
        if (factory==null) initGraphFactory();
        if (transactional) {
            return new DB(factory.getTx());
        } else {
            return new DB(factory.getNoTx());
        }
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
                e.printStackTrace();
            }
        }
    }

    /**
     * Closes every database in the pool and free all resources
     */
    public void closeAll() {
        this.factory.close();
    }

    /**
     * Returns the factory used to create databases
     * @return
     */
    public OrientGraphFactory getFactory() throws IOException {
        if (this.factory==null) initGraphFactory();
        return this.factory;
    }

    public void drop() throws IOException {
        if (remoteServer!=null) {
            remoteServer.connect(this.config.getUsername(), this.config.getPassword());
            remoteServer.dropDatabase(this.config.getDatabase());
        }
        if (memoryServer!=null) {
            memoryServer.getStorage().delete();
        }
    }
}
