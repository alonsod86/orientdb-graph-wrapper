package fs.orientdb;

/**
 * Configuration file for the factory
 * Created by dgutierrez on 23/5/15.
 */
public class OrientConfiguration {
    public static final Integer DEFAULT_MIN_POOL = 1;
    public static final Integer DEFAULT_MAX_POOL = 10;

    public static final String DATABASE_MEMORY = "memory";
    public static final String DATABASE_LOCAL = "plocal";
    public static final String DATABASE_REMOTE = "remote";

    // Database path. It can be a physical directory or a remote url
    private String database;

    // Schema. The schema helps to isolate data by creating different databases
    private String schema;
    private Integer minPool;
    private Integer maxPool;

    private String username;
    private String password;

    // By default the database will be allocated in memory
    private String databaseType = DATABASE_MEMORY;

    public OrientConfiguration(String database, String schema, Integer minPool, Integer maxPool, String username, String password, String databaseType) {
        this.database = database;
        this.schema = schema;
        this.minPool = minPool;
        this.maxPool = maxPool;
        this.username = username;
        this.password = password;
        this.databaseType = databaseType;
    }

    public OrientConfiguration(String database, String schema, Integer minPool, Integer maxPool, String username, String password) {
        this.database = database;
        this.schema = schema;
        this.minPool = minPool;
        this.maxPool = maxPool;
        this.username = username;
        this.password = password;
    }

    public OrientConfiguration(String database, String schema, Integer minPool, Integer maxPool) {
        this.database = database;
        this.schema = schema;
        this.minPool = minPool;
        this.maxPool = maxPool;
    }

    public OrientConfiguration(String database, String schema, Integer minPool, Integer maxPool, String databaseType) {
        this.database = database;
        this.schema = schema;
        this.minPool = minPool;
        this.maxPool = maxPool;
        this.databaseType = databaseType;
    }

    public OrientConfiguration(String database, String schema) {
        this.database = database;
        this.schema = schema;
        this.minPool = DEFAULT_MIN_POOL;
        this.maxPool = DEFAULT_MAX_POOL;
    }

    public OrientConfiguration(String database) {
        this.database = database;
    }

    public OrientConfiguration(String database, String schema, String username, String password) {
        this.database = database;
        this.schema = schema;
        this.username = username;
        this.password = password;
        this.minPool = DEFAULT_MIN_POOL;
        this.maxPool = DEFAULT_MAX_POOL;
    }

    public OrientConfiguration() {}

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Integer getMinPool() {
        return minPool;
    }

    public void setMinPool(Integer minPool) {
        this.minPool = minPool;
    }

    public Integer getMaxPool() {
        return maxPool;
    }

    public void setMaxPool(Integer maxPool) {
        this.maxPool = maxPool;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }
}
