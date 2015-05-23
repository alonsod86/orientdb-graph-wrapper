package es.fs.orientdb;

import java.util.Hashtable;

/**
 * Created by girigoyen on 17/04/2015.
 */
public class Relation {

    private String name;
    private String in;
    private String out;
    private String classNameIn;
    private String classNameOut;
    private Hashtable<String,String> attributes;

    public Relation(String name, String in, String out, Hashtable<String, String> attributes) {
        this.name = name;
        this.in = in;
        this.out = out;
        this.attributes = attributes;
    }

    public Relation(String name, String in, String out, String classNameIn, String classNameOut) {
        this.name = name;
        this.in = in;
        this.out = out;
        this.classNameIn = classNameIn;
        this.classNameOut = classNameOut;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIn() {
        return in;
    }

    public void setIn(String in) {
        this.in = in;
    }

    public String getOut() {
        return out;
    }

    public void setOut(String out) {
        this.out = out;
    }

    public Hashtable<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Hashtable<String, String> attributes) {
        this.attributes = attributes;
    }

    public String getClassNameIn() {
        return classNameIn;
    }

    public void setClassNameIn(String classNameIn) {
        this.classNameIn = classNameIn;
    }

    public String getClassNameOut() {
        return classNameOut;
    }

    public void setClassNameOut(String classNameOut) {
        this.classNameOut = classNameOut;
    }
}
