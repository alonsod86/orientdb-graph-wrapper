package es.fs.orientdb;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


/**
 * Created by girigoyen on 21/04/2015.
 */
public class OrientDBInterface {
    private String databaseURL;
    private String userName;
    private String pass;
    private String schema;
    private OrientGraphFactory factory;
    private OrientGraphNoTx graphDB;
    private HashMap <String, List<String>> hashPKeys;

    public OrientDBInterface(String databaseURL, String schema, String userName, String pass) {
        this.databaseURL = databaseURL;
        this.userName = userName;
        this.pass = pass;
        this.schema = schema;

        try{
            OServerAdmin server = new OServerAdmin(databaseURL + "/" + schema).connect(userName, pass);

            if (!server.existsDatabase("plocal")) {
                server.createDatabase("graph", "plocal");
            }
            server.close();
            this.factory = new OrientGraphFactory(databaseURL + "/" + schema, userName, pass);
        }catch (Exception e){
            e.printStackTrace();
        }
        if (factory != null){
            graphDB = factory.getNoTx();
        }

    }

    /**
     * Method to find out if a class is defined in the database
     * @param className
     * @return boolean: if the class exists
     */
    public boolean existClass(String className){
        return (existClass(className, null, false));
    }

    public boolean existClass (String className, String pKey){
        return (existClass(className, pKey, false));
    }

    /**
     * Method to find out if a class is defined in the database. It may create it.
     * @param className
     * @param createIt boolean If true, the class is created if doesn't exist
     * @return boolean if the class exists (or has been created)
     */
    public boolean existClass(String className, String pKey, boolean createIt){
        OrientVertexType vertexType = graphDB.getVertexType(className);
        if (vertexType == null && createIt){
            vertexType = graphDB.createVertexType(className, "V");
            if (pKey != null){
                vertexType.createProperty(pKey, OType.STRING);
                vertexType.createIndex(className + "." + className + "_id", OClass.INDEX_TYPE.UNIQUE, pKey);
            }
        }
        return (vertexType != null);
    }

    /**
     * Method to find nodes in the database.
     * @param className
     * @param id
     * @return boolean. If the nodes exists or not.
     */
    public Vertex existNode (String className, String id){
        Vertex nodus = null;
        try{
            nodus = this.graphDB.getVertexByKey(className + "." + className + "_id", id);
            return nodus;
        }catch (IllegalArgumentException iae){
            return null;
        }
    }

    /**
     * Method to compare a node with new values. If any value has change or is new, return true
     * @param node
     * @param newValues
     * @return
     */
    public boolean nodeHasChanged (Vertex node, HashMap<String, String> newValues){
        for (String key : newValues.keySet()){
            String prop = node.getProperty(key);
            if (prop == null){
                return true; //if the new attribute is new, it's a change
            }else if (!prop.equals(newValues.get(key))){
                return true; //if it's not equal, it's a change
            }
        }
        return false;
    }

    /**
     * Method to create a node in the database. If already exist, does nothing.
     * @param values
     * @param className
     * @param id
     * @return the created node or NULL
     */
    public Vertex createNode (HashMap<String, String> values, String className, String id){
       return this.createNode(values, className, id, false);
    }

    /**
     *  Method to create a node in the database. If already exist, may overwrite.
     * @param values
     * @param className
     * @param id
     * @param overWrite
     * @return the created node or NULL
     */
    public Vertex createNode (HashMap<String, String> values, String className, String id, boolean overWrite){
        Vertex node = this.existNode(className, id);
        if (node == null){
            node = this.graphDB.addVertex("class:" + className);
            for (String key : values.keySet()){
                node.setProperty(key, values.get(key));
            }
        }else if (node != null && overWrite){
            this.nodeUpdate(node, values);
        }
        return node;
    }

    /**
     * Updates the content of the node.
     * @param node
     * @param values
     */
    public void nodeUpdate (Vertex node, HashMap<String, String> values){
        this.nodeUpdate(node, values, false);
    }

    /**
     * Updates the content of the node. May delete previous data
     * @param node
     * @param values
     */
    public void nodeUpdate (Vertex node, HashMap<String, String> values, boolean clearIt){
        if (clearIt){
            for (String key : node.getPropertyKeys()){
                node.removeProperty(key);
            }
        }
        for (String key : values.keySet()){
            node.setProperty(key, values.get(key));
        }
    }

    /**
     * Checks if a relation class exists
     * @param name the name of relation
     * @return
     */

    public boolean existRelationClass (String name){
        return this.existRelationClass (name, false);
    }

    /**
     * Checks if a relation class exists. If not, it may be created
     * @param name the name of relation
     * @param createIt if it should be created if doesn't exist
     * @return
     */

    public boolean existRelationClass(String name, boolean createIt) {
        OrientEdgeType edgeType = graphDB.getEdgeType(name);
        if (edgeType == null && createIt){
            edgeType = graphDB.createEdgeType(name, "E");
        }
        return (edgeType != null);
    }

    /**
     * Search for a relation between two nodes and with concrete name
     * @param inNode
     * @param outNode
     * @param name
     * @return
     */
    public Edge existRelation (Vertex inNode, Vertex outNode, String name){
        return this.existRelation(inNode, outNode, name, false, null);
    }

    /**
     *  * Search for a relation between two nodes and with concrete name. If doesn't exist, may create it
     * @param inNode
     * @param outNode
     * @param name
     * @param createIt
     * @param values the attributes of the relation.
     * @return
     */
    public Edge existRelation (Vertex inNode, Vertex outNode, String name, boolean createIt, HashMap<String, String> values){
        OCommandSQL sql = new OCommandSQL("SELECT * FROM " + name + " WHERE out=\"" + outNode.getId() + "\" AND in=\"" + inNode.getId() + "\"");
        OrientDynaElementIterable lEdges = this.graphDB.command(sql).execute();
        Iterator itr = lEdges.iterator();
        while(itr.hasNext()) {
            return (Edge) itr.next();
        }
        if (createIt){
           return createRelation(inNode, outNode, name, values);
        }
        return null;
    }

    /**
     * Create a relation between 2 nodes.
     * @param inNode
     * @param outNode
     * @param name
     * @param attribs
     * @return The edge representing the relatin created. Null if can't create it.
     */
    public Edge createRelation (Vertex inNode, Vertex outNode, String name, HashMap<String, String> attribs){
        try{
            Edge edge = outNode.addEdge(name, inNode);
            if (attribs != null){
                for (String key : attribs.keySet()){
                    edge.setProperty(key, attribs.get(key));
                }
            }
            return edge;
        }catch (Exception e){
            //nothing to do
        }
        return null;
    }

    public boolean relationHasChanged (Edge relation, HashMap<String, String> values){
        for (String key : values.keySet()){
            String prop = relation.getProperty(key);
            if (prop == null){
                return true; //if the new attribute is new, it's a change
            }else if (!prop.equals(values.get(key))){
                return true; //if it's not equal, it's a change
            }
        }
        return false;
    }

    public void relationUpdate (Edge relation, HashMap<String, String> values){
        relationUpdate(relation, values, false);
    }

    public void relationUpdate (Edge relation, HashMap<String, String> values, boolean clearIt){
        if (clearIt){
            for (String key : relation.getPropertyKeys()){
                relation.removeProperty(key);
            }
        }
        for (String key : values.keySet()){
            relation.setProperty(key, values.get(key));
        }
    }

    public OrientDynaElementIterable executeQuery (String sqlQuery) {
        try{
            OCommandSQL sql = new OCommandSQL(sqlQuery);
            OrientDynaElementIterable result = this.graphDB.command(sql).execute();
            return result;
        }catch (Exception e){
            return null;
        }
    }

    /**
     * Get all nodes related with the one passed.
     * @param vertex node whose relations want to know
     * @param direc direction of the relations: 1=OUT, 2=IN, Other:BOTH;
     * @return
     */
    public Iterable<Vertex> getNodesRelated (Vertex vertex, int direc){
        return getNodesRelated(vertex, direc, null);
    }

    /**
     * Get all nodes related with the one passed.
     * @param vertex  node whose relations want to know
     * @param direc direction of the relations: 1=OUT, 2=IN, Other:BOTH;
     * @param relName name of the relation, to filter nodes. If none, all the relations.
     * @return
     */
    public Iterable<Vertex> getNodesRelated (Vertex vertex, int direc, String relName){
        Direction direction = Direction.BOTH;
        if (direc == 1){
            direction = Direction.OUT;
        }else if (direc == 2){
            direction = Direction.IN;
        }
        if (relName != null){
            return vertex.getVertices(direction, relName);
        }else {
            return vertex.getVertices(direction);
        }
    }

    public Iterable<Edge> getRelations (Vertex vertex, int direc){
        Direction direction = Direction.BOTH;
        if (direc == 1){
            direction = Direction.OUT;
        }else if (direc == 2){
            direction = Direction.IN;
        }
        return vertex.getEdges(direction);
    }

    public List<String> getRelationsNames (Vertex vertex, int direc){
        Direction direction = Direction.BOTH;
        if (direc == 1){
            direction = Direction.OUT;
        }else if (direc == 2){
            direction = Direction.IN;
        }
        List<String> lEdgeNames = new ArrayList<String>();
        Iterator<Edge> edgeIterable = vertex.getEdges(direction).iterator();
        while (edgeIterable.hasNext()) {
            String relName = edgeIterable.next().getLabel();
            if (!lEdgeNames.contains(relName)){
                lEdgeNames.add(relName);
            }
        }

        return lEdgeNames;
    }





    //------------------------ GETTERS AND SETTERS---------------------------------



    public String getDatabaseURL() {
        return databaseURL;
    }

    public void setDatabaseURL(String databaseURL) {
        this.databaseURL = databaseURL;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public OrientGraphFactory getFactory() {
        return factory;
    }

    public void setFactory(OrientGraphFactory factory) {
        this.factory = factory;
    }

    public OrientGraphNoTx getGraphDB() {
        return graphDB;
    }

    public void setGraphDB(OrientGraphNoTx graphDB) {
        this.graphDB = graphDB;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public HashMap<String, List<String>> getHashPKeys() {
        return hashPKeys;
    }

    public void setHashPKeys(HashMap<String, List<String>> hashPKeys) {
        this.hashPKeys = hashPKeys;
    }
}
