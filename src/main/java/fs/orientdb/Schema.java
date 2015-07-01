package fs.orientdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Schema implementation for OrientDB graph database
 * Created by dgutierrez on 23/5/15.
 */
public class Schema {
    // Schema name
    private String className;
    // Instance to parent graph database
    private OrientBaseGraph graphDB;

    public Schema(String schema, OrientBaseGraph db) {
        this.className = schema;
        this.graphDB = db;
    }

    /**
     * Method to find nodes in the database.
     * @return boolean. If the nodes exists or not.
     */
    public Vertex existNode(String key, Object value){
        try{
        	Iterable<Vertex> vertices = this.graphDB.getVertices(key, value);
        	if (vertices!=null) {
        		Iterator<Vertex> it = vertices.iterator();
        		if (it!=null && it.hasNext()) {
        			return it.next();
        		}
        	}
            return null;
        }catch (Exception iae){
            return null;
        }
    }

    public Vertex existNode(Pk pk) {
        return existNode(pk.key, pk.value);
    }

    /**
     * Method to compare a node with new values. If any value has change or is new, return true
     * @param node
     * @param newAttributes
     * @return
     */
    public boolean nodeHasChanged(Vertex node, HashMap<String, Object> newAttributes) {
        for (String key : newAttributes.keySet()) {        	
    		if (newAttributes.get(key) != null){
	            Object prop = node.getProperty(key);
	            if (prop == null) {
	                return true; //if the new attribute is new, it's a change
	            } else if (!prop.toString().equals(newAttributes.get(key))) {
	                return true; //if it's not equal, it's a change
	            }
        	}       		        	        	
        }
        return false;
    }
    
    public boolean nodeHasChanged(Vertex node, HashMap<String, Object> newAttributes, String... excluded) {
    	 for (String key : newAttributes.keySet()) {
    		 if (!Arrays.asList(excluded).contains(key)){    			     		 
	     		if (newAttributes.get(key) != null){
	 	            Object prop = node.getProperty(key);
	 	            if (prop == null) {
	 	                return true; //if the new attribute is new, it's a change
	 	            } else if (!prop.toString().equals(newAttributes.get(key).toString())) {
	 	                return true; //if it's not equal, it's a change
	 	            }
	         	}
    		 }
         }
    	return false;
    }
    /**
     * Create a new node using a primary key and some attributes. If overWrite is true, it will try to find
     * it by its pk and then update it's attributes
     * @param pk
     * @param attributes
     * @param overWrite
     * @return
     */
    public Vertex createNode(Pk pk, HashMap<String, Object> attributes, boolean overWrite) {
        Vertex node = this.existNode(pk.key, pk.value);
        if (node == null){
            node = this.graphDB.addVertex("class:" + className);
            node.setProperty(pk.key, pk.value);

            if (attributes!=null) {
                for (String key : attributes.keySet()) {                	
            		node.setProperty(key, attributes.get(key));                	
                }
            }
        } else if (node != null && overWrite){
            this.updateNode(node, attributes);
        }
        return node;
    }

    /**
     * Create a new node using primary key and attributes. If the node existed before nothing will happen
     * @param pk
     * @param attributes
     * @return
     */
    public Vertex createNode(Pk pk, HashMap<String, Object> attributes) {
        return createNode(pk, attributes, false);
    }

    /**
     * Create a new node using only a primary key
     * @param pk
     * @return
     */
    public Vertex createNode(Pk pk) {
        return createNode(pk, null, false);
    }

    /**
     * Updates the content of the node.
     * @param node
     * @param attributes
     */
    public void updateNode(Vertex node, HashMap<String, Object> attributes){
        this.updateNode(node, attributes, false);
    }

    /**
     * Updates the content of the node. May delete previous data
     * @param node
     * @param attributes
     */
    public void updateNode(Vertex node, HashMap<String, Object> attributes, boolean clearIt){    	
        if (clearIt){
            for (String key : node.getPropertyKeys()){
                node.removeProperty(key);
            }
        }
        for (String key : attributes.keySet()){
            node.setProperty(key, attributes.get(key));
        }
    }

    /**
     * Get all nodes related with the one passed.
     * @param vertex node whose relations want to know
     * @param direction of the relations: 1=OUT, 2=IN, Other:BOTH;
     * @return
     */
    public Iterable<Vertex> getNodesRelated (Vertex vertex, Direction direction){
        return getNodesRelated(vertex, direction, null);
    }

    /**
     * Get all nodes related with the one passed.
     * @param vertex  node whose relations want to know
     * @param direction of the relations: 1=OUT, 2=IN, Other:BOTH;
     * @param relName name of the relation, to filter nodes. If none, all the relations.
     * @return
     */
    public Iterable<Vertex> getNodesRelated (Vertex vertex, Direction direction, String relName){
        if (relName != null){
            return vertex.getVertices(direction, relName);
        }else {
            return vertex.getVertices(direction);
        }
    }

    /**
     * Returns every relationship of a vertex given its direction
     * @param vertex
     * @param direction
     * @return
     */
    public Iterable<Edge> getRelations (Vertex vertex, Direction direction){
        return vertex.getEdges(direction);
    }

    /**
     * Returns the name of the relationships around a vertex given the direction
     * @param vertex
     * @param direction
     * @return
     */
    public List<String> getRelationsNames (Vertex vertex, Direction direction){
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

    /**
     * Creates a unique index for a field
     * @param type
     * @param field
     */
    public void createIndex(OType type, String field) {
        OrientVertexType vertexType = graphDB.getVertexType(className);
        if (vertexType == null) {
            vertexType = graphDB.createVertexType(className, "V");
        }

        vertexType.createProperty(field, type);
        vertexType.createIndex(className + "." + field, OClass.INDEX_TYPE.UNIQUE, field);
    }

    
    public Set<OIndex<?>> getIndexes() {
        OrientVertexType vertexType = graphDB.getVertexType(className);
        return vertexType.getIndexes();
    }	
}