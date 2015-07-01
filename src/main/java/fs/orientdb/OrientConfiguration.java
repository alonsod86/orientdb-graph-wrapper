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
    private String url;

    private Integer minPool;
    private Integer maxPool;

    private String username;
    private String password;

    // By default the database will be allocated in memory
    private String databaseType = DATABASE_MEMORY;

    public OrientConfiguration(String url, Integer minPool, Integer maxPool, String username, String password, String databaseType) {
        this.url = url;
        this.minPool = minPool;
        this.maxPool = maxPool;
        this.username = username;
        this.password = password;
        this.databaseType = databaseType;
    }

    public OrientConfiguration(String url, Integer minPool, Integer maxPool, String username, String password) {
        this.url = url;
        this.minPool = minPool;
        this.maxPool = maxPool;
        this.username = username;
        this.password = password;
    }

    public OrientConfiguration(String url, Integer minPool, Integer maxPool) {
        this.url = url;
        this.minPool = minPool;
        this.maxPool = maxPool;
    }

    public OrientConfiguration(String url, Integer minPool, Integer maxPool, String databaseType) {
        this.url = url;
        this.minPool = minPool;
        this.maxPool = maxPool;
        this.databaseType = databaseType;
    }

    public OrientConfiguration(String url) {
        this.url = url;
        this.minPool = DEFAULT_MIN_POOL;
        this.maxPool = DEFAULT_MAX_POOL;
    }

    public OrientConfiguration(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.minPool = DEFAULT_MIN_POOL;
        this.maxPool = DEFAULT_MAX_POOL;
    }

    public OrientConfiguration() {}

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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
