package fs.orientdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Schema implementation for OrientDB graph database
 * Created by dgutierrez on 23/5/15.
 */
public class Collection {
    // Schema name
    private String className;
    // Instance to parent graph database
    private OrientBaseGraph graphDB;

    public Collection(String schema, OrientBaseGraph db) {
        this.className = schema;
        this.graphDB = db;
    }

    /**
     * Method to find nodes in the database.
     * @return boolean. If the nodes exists or not.
     */
    public Vertex existNode(String key, Object value){
        Vertex node = null;
        try{
            node = this.graphDB.getVertexByKey(className + "." + key, value);
            return node;
        }catch (IllegalArgumentException iae){
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
            String prop = node.getProperty(key);
            if (prop == null) {
                return true; //if the new attribute is new, it's a change
            } else if (!prop.equals(newAttributes.get(key))) {
                return true; //if it's not equal, it's a change
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
     * Checks if a relation class exists
     * @param name the name of relation
     * @return
     */

    public boolean existRelationClass(String name){
        return this.existRelationClass(name, false);
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
     * Search for a relation between two nodes and with a given name
     * @param inNode
     * @param outNode
     * @param name
     * @return
     */
    public Edge existRelation(Vertex inNode, Vertex outNode, String name){
        return this.existRelation(inNode, outNode, name, false, null);
    }

    /**
     * Search for a relation between two nodes and with a given name. If doesn't exist, may create it
     * @param inNode
     * @param outNode
     * @param name
     * @param createIt
     * @param attributes the attributes of the relation.
     * @return
     */
    public Edge existRelation (Vertex inNode, Vertex outNode, String name, boolean createIt, HashMap<String, Object> attributes){
        OCommandSQL sql = new OCommandSQL("SELECT * FROM " + name + " WHERE out=\"" + outNode.getId() + "\" AND in=\"" + inNode.getId() + "\"");
        OrientDynaElementIterable lEdges = this.graphDB.command(sql).execute();
        Iterator itr = lEdges.iterator();
        while(itr.hasNext()) {
            return (Edge) itr.next();
        }
        if (createIt){
            return createRelation(inNode, outNode, name, attributes);
        }
        return null;
    }

    /**
     * Create a relation between two nodes.
     * @param inNode
     * @param outNode
     * @param name
     * @param attributes
     * @return The edge representing the relation created. Null if can't create it.
     */
    public Edge createRelation (Vertex inNode, Vertex outNode, String name, HashMap<String, Object> attributes){
        try{
            Edge edge = outNode.addEdge(name, inNode);
            if (attributes != null){
                for (String key : attributes.keySet()){
                    edge.setProperty(key, attributes.get(key));
                }
            }
            return edge;
        }catch (Exception e){
            //nothing to do
        }
        return null;
    }

    public Edge createRelation (Vertex inNode, Vertex outNode, String name){
        return createRelation(inNode, outNode, name, null);
    }

    /**
     * Checks if a relationship has changed given the Original Edge and the new attributes
     * @param relation
     * @param attributes
     * @return
     */
    public boolean relationHasChanged (Edge relation, HashMap<String, Object> attributes){
        for (String key : attributes.keySet()){
            String prop = relation.getProperty(key);
            if (prop == null){
                return true; //if the new attribute is new, it's a change
            }else if (!prop.equals(attributes.get(key))){
                return true; //if it's not equal, it's a change
            }
        }
        return false;
    }

    /**
     * Updates a relationship with the given attributes
     * @param relation
     * @param attributes
     */
    public Edge relationUpdate (Edge relation, HashMap<String, Object> attributes){
        return relationUpdate(relation, attributes, false);
    }

    /**
     * Updates a relationship with the given attributes, overwriting everything
     * @param relation
     * @param attributes
     * @param clearIt
     */
    public Edge relationUpdate (Edge relation, HashMap<String, Object> attributes, boolean clearIt){
        if (clearIt){
            for (String key : relation.getPropertyKeys()){
                relation.removeProperty(key);
            }
        }
        for (String key : attributes.keySet()){
            relation.setProperty(key, attributes.get(key));
        }

        return relation;
    }

    /**
     * Provides a direct Query Executor using SQL
     * @param sqlQuery
     * @return
     */
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

        vertexType.createProperty(field, OType.STRING);
        vertexType.createIndex(className + "." + field, OClass.INDEX_TYPE.UNIQUE, field);
    }

    public Set<OIndex<?>> getIndexes() {
        OrientVertexType vertexType = graphDB.getVertexType(className);
        return vertexType.getIndexes();
    }
}
