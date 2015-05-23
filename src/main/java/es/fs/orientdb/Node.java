package es.fs.orientdb;
import java.util.Hashtable;

/**
 * Created by girigoyen on 17/04/2015.
 */
public class Node {
    private String className;
    private String id;
    private Hashtable<String,String> attributes;

    public Node(String className, String id, Hashtable<String, String> attributes) {
        this.className = className;
        this.id = id;
        this.attributes = attributes;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Hashtable<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Hashtable<String, String> attributes) {
        this.attributes = attributes;
    }
}
