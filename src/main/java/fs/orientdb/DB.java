package fs.orientdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Database instance with the interface to OrientDB
 * Created by dgutierrez on 23/5/15.
 */
public class DB {
	static Logger log = LoggerFactory.getLogger(DB.class.getSimpleName());

	// Instance to a transactional graph database
	private OrientGraph txGraph;

	// Instance to a non transactional graph database
	private OrientGraphNoTx regularGraph;

	// Global instance to the currently created database
	private OrientBaseGraph graphDB;

	// Instance to the factory used to create instances to database
	private OrientGraphFactory factory;

	// JSON serializer
	private ObjectMapper json = new ObjectMapper();

	/**
	 * Create an active connection using the factory that contains all the information necessary to connect to an Orientdb database
	 * @param factory
	 * @param transactional
	 */
	public DB(OrientGraphFactory factory, boolean transactional) {
		this.factory = factory;
		if (transactional) {
			this.txGraph = factory.getTx();
			this.graphDB = this.txGraph;
		} else {
			this.regularGraph = factory.getNoTx();
			this.graphDB = this.regularGraph;
		}
	}

	/**
	 * Returns an interface to work with Orientdb classes
	 * @param Schema
	 * @return
	 */
	public Schema getSchema(String Schema) {
		return new Schema(Schema, this);
	}

	/**
	 * Closes the connection and returns the database to the pool.
	 */
	public void close() {
		this.graphDB.shutdown();
	}

	/**
	 * Returns true if this database is transactional
	 * @return
	 */
	public boolean isTransactional() {
		return this.txGraph != null;
	}

	/**
	 * Begins a transaction only on a transactional connection
	 */
	public void begin() {
		if (isTransactional()) {
			this.txGraph.begin();
		} else {
			log.warn("Commit is not necessary with a non transactional connection");
		}
	}

	/**
	 * Commits a transaction only on a transactional connection
	 */
	public void commit() {
		if (isTransactional()) {
			this.txGraph.commit();
		} else {
			log.warn("Commit is not necessary with a non transactional connection");
		}
	}

	/**
	 * Rolls back a transaction only on a transactional connection
	 */
	public void rollback() {
		if (isTransactional()){
			this.txGraph.rollback();
		} else {
			log.warn("Rollback won't work on a non transactional connection");
		}
	}

	/**
	 * Browses a class within the current database
	 * @param Schema
	 * @return
	 */
	public ORecordIteratorClass<ODocument> browseClass(String Schema){
		return this.factory.getDatabase().browseClass(Schema);    		
	}

	/**
	 * Executes a SQL query returning a result set of ODocuments
	 * @param sql
	 * @return
	 */
	public List<ODocument> query(String sql){
		return this.factory.getDatabase().query(new OSQLSynchQuery<ODocument> (sql));
	}

	/**
	 * Creates a new class with a String property
	 * @param className
	 * @param pKey
	 * @return
	 */
	public OrientVertexType createClass (String className, String pKey){
		try{
			OrientVertexType vertexType = graphDB.createVertexType(className, "V");
			if (pKey != null){						
				vertexType.createProperty(pKey, OType.STRING);
				vertexType.createIndex(className + "." + pKey, OClass.INDEX_TYPE.UNIQUE, pKey);						
			}
			return vertexType;
		}catch (Exception e){
			log.error("Could not create class of type {} on database {}. Reason is {}", className, getDatabaseName(), e.getMessage());
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
		try {
			OrientVertexType vertexType = graphDB.getVertexType(className);
			if (vertexType == null && createIt){
				vertexType = createClass(className, pKey);
			}
			return (vertexType != null);
		} catch (Exception e) {
			log.error("Could not check existence of class type {} on database {}. Reason is {}", className, getDatabaseName(), e.getMessage());
			return false;
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
	 * @param className the name of relation
	 * @param createIt if it should be created if doesn't exist
	 * @return
	 */

	public boolean existRelationClass(String className, boolean createIt) {
		try {
			OrientEdgeType edgeType = graphDB.getEdgeType(className);
			if (edgeType == null && createIt){
				edgeType = createRelationClass(className);
			}
			return (edgeType != null);
		} catch (Exception e) {
			log.error("Could not check existence of relationship class type {} on database {}. Reason is {}", className, getDatabaseName(), e.getMessage());
			return false;
		}
	}

	/**
	 * Creates a new relation class for edges
	 * @param name
	 * @return
	 */
	public OrientEdgeType createRelationClass(String name){
		try {
			OrientEdgeType edgeType = graphDB.createEdgeType(name, "E");		
			return edgeType;
		} catch (Exception e) {
			log.error("Could not create class for relationship type {} on database {}. Reason is {}", name, getDatabaseName(), e.getMessage());
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
	 * @param pkValue
	 * @return
	 */
	public Vertex existNode(String className, String key, Object value){
		try {
			OrientDynaElementIterable lVertices = this.executeQuery("SELECT FROM " + className + " WHERE " + key + " LIKE '" + value + "'");
			Vertex v = null;
			for (Object vertex : lVertices){
				v = (Vertex) vertex;				
			}
			return v;
		} catch (Exception e) {
			log.error("Could not check if node {} exists on database {}. Reason is {}", key+":"+value, getDatabaseName(), e.getMessage());
			return null;
		}

	}

	/**
	 * Return a node with hte pk of the passed class
	 * @param className
	 * @param pkValue
	 * @return
	 */
	public Vertex existNode(String key, Object value){
		return existNode("V", key, value);
	}

	/**
	 * Checks the existence of a node by its Pk
	 * @param pk
	 * @return
	 */
	public Vertex existNode(Pk pk) {
		return existNode(pk.key, pk.value);
	}
	
	/**
	 * Method to find nodes in the database using an index
	 * @return boolean. If the nodes exists or not.
	 */
	public Vertex existNodeIndex(Object value, String index) {
		try {
			String val = null;
			if (value instanceof String) {
				val = "\"" + value + "\"";
			} else {
				val = value.toString();
			}
			OrientDynaElementIterable vertex = executeQuery("SELECT rid as node FROM index:" + index + " WHERE key = " + val);
			OrientVertex v = (OrientVertex) vertex.iterator().next();
			if (v!=null) {
				return v.getProperty("node");
			}
		} catch (Exception e) {
			log.error("Could not check existence of node in database {}. Reason is {}", getDatabaseName(), e.getMessage());
			return null;
		}
		return null;
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
	public Edge existRelation (Vertex inNode, Vertex outNode, String name, boolean createIt, HashMap<String, ?> attributes){
		try {
			OCommandSQL sql = new OCommandSQL("SELECT * FROM " + name + " WHERE out=\"" + outNode.getId() + "\" AND in=\"" + inNode.getId() + "\"");
			OrientDynaElementIterable lEdges = this.graphDB.command(sql).execute();
			Iterator<Object> itr = lEdges.iterator();
			if(itr.hasNext()) {
				return (Edge) itr.next();
			}
			if (createIt){
				return createRelation(inNode, outNode, name, attributes);
			}
		} catch (Exception e) {
			log.error("Could not check existence of relationship {} - {} - {} on database {}. Reason is {}", outNode.getId(), name, inNode.getId(), getDatabaseName(), e.getMessage());
		}
		return null;
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
	public Edge existRelation (Pk in, Pk out, String name, HashMap<String, ?> attributes){
		try {
			OCommandSQL sql = new OCommandSQL("SELECT FROM " + name + " WHERE out IN (SELECT FROM V WHERE " + out.toQuery() + ") AND in IN (SELECT FROM V WHERE " + in.toQuery() + ")");
			OrientDynaElementIterable lEdges = this.graphDB.command(sql).execute();
			Iterator<Object> itr = lEdges.iterator();
			if(itr.hasNext()) {
				return (Edge) itr.next();
			}
		} catch (Exception e) {
			log.error("Could not check existence of relationship {} - {} - {} on database {}. Reason is {}", out, name, in, getDatabaseName(), e.getMessage());
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
	public Edge createRelation (Vertex inNode, Vertex outNode, String name, HashMap<String, ?> attributes){
		try{
			String mapAsJson = null;
			String query = null;
			if (attributes!=null) {
				// add pk to the attributes hashmap
				mapAsJson = json.writeValueAsString(attributes);
				query = "CREATE EDGE " + name + " FROM "+outNode.getId()+" TO "+inNode.getId() +" CONTENT " + mapAsJson + " RETRY 3 WAIT 1";
			} else {
				query = "CREATE EDGE " + name + " FROM "+outNode.getId()+" TO "+inNode.getId() + " RETRY 3 WAIT 1";
			}

			OCommandSQL sql = new OCommandSQL(query);
			OrientDynaElementIterable result = getTinkerpopInstance().command(sql).execute();
			return (Edge) result.iterator().next();

		} catch (ORecordDuplicatedException e) {
			log.error("Could not create relationship {} - {} - {} on database. Reason is {}", inNode.getId(), name, outNode.getId(), getDatabaseName(), "DUPLICATED EDGE");
		} catch (Exception e) {
			log.error("Could not create relationship {} - {} - {} on database. Reason is {}", inNode.getId(), name, outNode.getId(), getDatabaseName(), e.getMessage());
		}
		return null;
	}

	public Edge createRelation (Pk in, Pk out, String name, HashMap<String, ?> attributes) throws Exception {
		// add pk to the attributes hashmap
		String mapAsJson = json.writeValueAsString(attributes);

		String query = "CREATE EDGE " + name + " FROM (SELECT FROM V WHERE " + out.toQuery() + ") TO (SELECT FROM V WHERE " + in.toQuery() + ") CONTENT " + mapAsJson + " RETRY 3 WAIT 1" ;
		OCommandSQL sql = new OCommandSQL(query);
		OrientDynaElementIterable result = getTinkerpopInstance().command(sql).execute();
		return (Edge) result.iterator().next();
	}

	/**
	 * Create a relation between two vertices with a relationship name
	 * @param inNode
	 * @param outNode
	 * @param name
	 * @return
	 */
	public Edge createRelation (Vertex inNode, Vertex outNode, String name){
		return createRelation(inNode, outNode, name, null);
	}

	/**
	 * Checks if a relationship has changed given the Original Edge and the new attributes
	 * @param relation
	 * @param attributes
	 * @return
	 */
	public boolean relationHasChanged (Edge relation, HashMap<String, ?> attributes){
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
	 * Checks if a relationship has changed given a set of attributes. It can exclude from comparison those in the list
	 * @param relation
	 * @param attributes
	 * @param excluded
	 * @return
	 */
	public boolean relationHasChanged (Edge relation, HashMap<String, ?> attributes, String...excluded){
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
	public Edge relationUpdate (Edge relation, HashMap<String, ?> attributes){
		return relationUpdate(relation, attributes, false);
	}

	/**
	 * Updates a relationship with the given attributes, overwriting everything
	 * @param relation
	 * @param attributes
	 * @param clearIt
	 */
	public Edge relationUpdate (Edge relation, HashMap<String, ?> attributes, boolean clearIt){
		try {
			if (clearIt){
				for (String key : relation.getPropertyKeys()){
					relation.removeProperty(key);
				}
			}
			for (String key : attributes.keySet()){
				relation.setProperty(key, attributes.get(key));
			}

			return relation;
		} catch (Exception e) {
			log.error("Could update relationship {} on database {}. Reason is {}", relation.getId(), getDatabaseName(), e.getMessage());
			return null;
		}
	}

	/**
	 * Drops a relationship using Pks of the nodes and the relationship name
	 * @param in
	 * @param out
	 * @param relationClass
	 * @return
	 */
	public int relationDrop (Pk in, Pk out, String relationClass) {
		String query = "DELETE EDGE " + relationClass + " FROM (SELECT FROM V WHERE " + out.toQuery() + ") TO (SELECT FROM V WHERE " + in.toQuery() + ")" ;
		OCommandSQL sql = new OCommandSQL(query);
		Integer removed = getTinkerpopInstance().command(sql).execute();
		return removed;
	}

	/**
	 * Drops a relationship using Pks of the nodes and the relationship name
	 * @param in
	 * @param out
	 * @param relationClass
	 * @return
	 */
	public int relationDrop (Vertex in, Vertex out, String relationClass) {
		String query = "DELETE EDGE " + relationClass + " FROM "+out.getId()+" TO " + in.getId();
		OCommandSQL sql = new OCommandSQL(query);
		Integer removed = getTinkerpopInstance().command(sql).execute();
		return removed;
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
	 * Returns the node represented by de rid 
	 * @param rid
	 * @return
	 */
	public Vertex getNode(String rid){
		try {
			return graphDB.getVertex(rid);
		} catch (Exception e) {
			log.error("Could not find node with @rid = {} on database {}. Reason is {}", rid, getDatabaseName(), e.getMessage());
			return null;
		}
	}

	/**
	 * Returns the original graph engine provided by the tinkerpop api
	 * @return
	 */
	public OrientBaseGraph getTinkerpopInstance() {
		return this.graphDB;
	}

	/**
	 * Returns the database name where this schema belongs to
	 * @return
	 */
	public String getDatabaseName() {
		try {
			return this.graphDB.getRawGraph().getName();
		} catch (Exception e) {
			return "ERROR_GET_DATABASE_NAME";
		}
	}

	public OrientGraphFactory getFactory() {
		return factory;
	}
}
