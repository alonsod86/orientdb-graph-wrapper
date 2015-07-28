package fs.orientdb;

/**
 * Created by tiocansino on 23/5/15.
 */
public class Pk {
    public String key;
    public Object value;

    public Pk(String key, Object value) {
        this.key = key;
        this.value = value;
    }
    
    @Override
    public String toString() {
    	return key + ": " + value;
    }
    
    public String toQuery() {
    	if (value instanceof Number) {
    		return key + "=" + value;
    	} else {
    		return key + "=\"" + value + "\"";
    	}
    }
}
