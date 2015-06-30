package fs.orientdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Database instance with the interface to OrientDB
 * Created by dgutierrez on 23/5/15.
 */
public class DB {
	
	static Logger log = LoggerFactory.getLogger(DB.class.getSimpleName());
	
	// Instance to transactional or non transactional graph database
	private OrientBaseGraph graphDB;

	public DB(OrientBaseGraph graph) {
		this.graphDB = graph;
	}

	public Collection getSchema(String schemaName) {
		return new Collection(schemaName, graphDB);
	}

	/**
	 * Closes the connection and returns the database to the pool
	 */
	public void close() {
		this.graphDB.shutdown();
	}

	/**
	 * Returns true if this database is transactional
	 * @return
	 */
	public boolean isTransactional() {
		return this.graphDB instanceof OrientGraph;
	}

	/**
	 * Returns the original graph engine provided by OrientDB
	 * @return
	 */
	public OrientBaseGraph getGraphEngine() {
		return this.graphDB;
	}

	
	public OrientVertexType createClass (String className, String pKey){
		try{
			OrientVertexType vertexType = graphDB.createVertexType(className, "V");
			if (pKey != null){						
				vertexType.createProperty(pKey, OType.STRING);
				vertexType.createIndex(className + "." + pKey, OClass.INDEX_TYPE.UNIQUE, pKey);						
			}
			return vertexType;
		}catch (Exception e){
			return null;
		}
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
			vertexType = createClass(className, pKey);
		}
		return (vertexType != null);
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
			edgeType = createRelationClass(name);
		}
		return (edgeType != null);
	}
	
	public OrientEdgeType createRelationClass(String name){
		try {
			OrientEdgeType edgeType = graphDB.createEdgeType(name, "E");		
			return edgeType;
		} catch (Exception e) {
			return null;
		}
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
	 * Return a node with hte pk of the passed class
	 * @param className
	 * @param pk
	 * @return
	 */
	public Vertex existNode(String className, String pk, String pkName){
		try {
			OrientDynaElementIterable lVertices = this.executeQuery("select * from " + className + " where " + pkName + " like '" + pk + "'");
			Vertex v = null;
			for (Object vertex : lVertices){
				v = (Vertex) vertex;				
			}
			return v;
		} catch (Exception e) {
			return null;
		}
		
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
		Iterator<Object> itr = lEdges.iterator();
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
			//edge.setProperty("name", name);
			if (attributes != null){
				for (String key : attributes.keySet()){
					edge.setProperty(key, attributes.get(key));
				}
			}
			return edge;
		}catch (Exception e){
			log.error(ExceptionUtils.getStackTrace(e));
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
	
	public boolean relationHasChanged (Edge relation, HashMap<String, Object> attributes, String...excluded){
		for (String key : attributes.keySet()){
			 if (!Arrays.asList(excluded).contains(key)){    	
				String prop = relation.getProperty(key).toString();
				if (prop == null){
					return true; //if the new attribute is new, it's a change
				}else if (!prop.equals(attributes.get(key).toString())){
					return true; //if it's not equal, it's a change
				}				 
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
	public OrientDynaElementIterable executeQuery (String sqlQuery) throws Exception {
		OCommandSQL sql = new OCommandSQL(sqlQuery);
		OrientDynaElementIterable result = this.graphDB.command(sql).execute();
		return result;
	}

	/**
	 * Method to find out if a class is defined in the database
	 * @param className
	 * @return boolean: if the class exists
	 */
	public boolean existClass(String className){
		return existClass(className, null, false);
	}

	public boolean existClass(String className, String pKey){
		return existClass(className, pKey, false);
	}

	/**
	 * REturns the node represented by de rid 
	 * @param nodeID
	 * @return
	 */
	public Vertex getNode (String nodeID){
		return graphDB.getVertex(nodeID);
	}
}
