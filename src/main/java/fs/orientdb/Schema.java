package fs.orientdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Schema implementation for OrientDB graph database
 * Created by dgutierrez on 23/5/15.
 */
public class Schema {
	static Logger log = LoggerFactory.getLogger(Schema.class.getSimpleName());

	// Schema name
	private String className;
	// Instance to parent graph database
	private DB db;
	// JSON serializer
	private ObjectMapper json = new ObjectMapper();
	
	public Schema(String schema, DB db) {
		this.className = schema;
		this.db = db;
	}

	/**
	 * Method to find nodes in the database.
	 * @return boolean. If the nodes exists or not.
	 */
	public Vertex existNode(String key, Object value) {
		try{
			Iterable<Vertex> vertices = this.db.getTinkerpopInstance().getVertices(key, value);
			
			if (vertices!=null) {
				Iterator<Vertex> it = vertices.iterator();
				if (it!=null && it.hasNext()) {
					return it.next();
				}
			}
			return null;
		} catch (Exception e) {
			log.error("Could not check existence of node {} in database {} and class {}. Reason is {}", key + ":" + value.toString(), getDatabaseName(), className, e.getMessage());
			return null;
		}
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
	 * Method to compare a node with new values. If any value has change or is new, return true
	 * @param node
	 * @param newAttributes
	 * @return
	 */
	public boolean nodeHasChanged(Vertex node, HashMap<String, ?> newAttributes) {
		try {
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
		} catch (Exception e) {
			log.error("Could not check if node {} has changed on database {} and class {}. Reason is {}", node.getId(), getDatabaseName(), className, e.getMessage());
			return false;
		}
	}

	/**
	 * Method to compare a node with new values, excluding those in the list
	 * @param node
	 * @param newAttributes
	 * @param excluded
	 * @return
	 */
	public boolean nodeHasChanged(Vertex node, HashMap<String, ?> newAttributes, String... excluded) {
		try {
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
		} catch (Exception e) {
			log.error("Could not check if node {} has changed {} on database {} and class {}. Reason is {}", node.getId(), getDatabaseName(), className, e.getMessage());
			return false;
		}
	}

	/**
	 * Create a new node using a primary key and some attributes. If overWrite is true, it will try to find
	 * it by its pk and then update it's attributes
	 * @param pk
	 * @param attributes
	 * @param overWrite
	 * @return
	 */
	public Vertex createNode(Pk pk, HashMap<String, ?> attributes, boolean overWrite) {
		try {
			Vertex node = this.existNode(pk.key, pk.value);
			if (node == null){
				node = this.db.getTinkerpopInstance().addVertex("class:" + className);
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
		} catch (Exception e) {
			log.error("Could not create node {} on database {}. Reason is {}", pk, getDatabaseName(), e.getMessage());
			return null;
		}
	}

	/**
	 * Create a new node using primary key and attributes. If the node existed before nothing will happen
	 * @param pk
	 * @param attributes
	 * @return
	 */
	public Vertex createNode(Pk pk, HashMap<String, ?> attributes) {
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
	 * Updates an existing node or creates a new one if does not exist, returning the old value of the node updated (or empty if it didn't exists(¡)
	 * @param pk
	 * @param attributes
	 * @return
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonGenerationException 
	 */
	public Vertex upsertNode(Pk pk, HashMap<String, Object> attributes) throws Exception {
		// add pk to attributes (the pk must be upserted too)
		attributes.put(pk.key, pk.value);
		// add pk to the attributes hashmap
		String mapAsJson = json.writeValueAsString(attributes);
		String valueAsType = "";
		if (pk.value instanceof Number) {
			valueAsType = pk.value.toString();
		} else {
			valueAsType = "\"" + pk.value.toString() + "\"";
		}
		String query = "UPDATE " + className + " MERGE " + mapAsJson + " UPSERT RETURN BEFORE WHERE " + pk.key + "=" + valueAsType;
		OCommandSQL sql = new OCommandSQL(query);
		OrientDynaElementIterable result = this.db.getTinkerpopInstance().command(sql).execute();
		return (Vertex) result.iterator().next();
	}

	/**
	 * Updates the content of the node.
	 * @param node
	 * @param attributes
	 */
	public void updateNode(Vertex node, HashMap<String, ?> attributes){
		this.updateNode(node, attributes, false);
	}

	/**
	 * Updates the content of the node. May delete previous data
	 * @param node
	 * @param attributes
	 */
	public void updateNode(Vertex node, HashMap<String, ?> attributes, boolean clearIt){
		try {
			if (node==null) throw new Exception("Vertex to update can not be null");
			if (clearIt){
				for (String key : node.getPropertyKeys()){
					node.removeProperty(key);
				}
			}
			for (String key : attributes.keySet()){
				node.setProperty(key, attributes.get(key));
			}
		} catch (Exception e) {
			log.error("Could not update node {} on database {}. Reason is {}", node.getId(), getDatabaseName(), e.getMessage());
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
		try {
			if (relName != null){
				return vertex.getVertices(direction, relName);
			} else {
				return vertex.getVertices(direction);
			}
		} catch (Exception e) {
			log.error("Could not get relationships from node {} of type {} and direction {} on database {}. Reason is {}", vertex.getId(), relName, direction, getDatabaseName(), e.getMessage());
			return null;
		}
	}

	/**
	 * Returns every relationship of a vertex given its direction
	 * @param vertex
	 * @param direction
	 * @return
	 */
	public Iterable<Edge> getRelations (Vertex vertex, Direction direction) {
		try {
			return vertex.getEdges(direction);
		} catch (Exception e) {
			log.error("Could not get relationships from node {} and direction {} on database {}. Reason is {}", vertex.getId(), direction, getDatabaseName(), e.getMessage());
			return null;
		}
	}

	/**
	 * Returns the name of the relationships around a vertex given the direction
	 * @param vertex
	 * @param direction
	 * @return
	 */
	public List<String> getRelationsNames (Vertex vertex, Direction direction) {
		try {
			List<String> lEdgeNames = new ArrayList<String>();
			Iterator<Edge> edgeIterable = vertex.getEdges(direction).iterator();
			while (edgeIterable.hasNext()) {
				String relName = edgeIterable.next().getLabel();
				if (!lEdgeNames.contains(relName)){
					lEdgeNames.add(relName);
				}
			}

			return lEdgeNames;
		} catch (Exception e) {
			log.error("Could not get relationships names from node {} and direction {} on database {}. Reason is {}", vertex.getId(), direction, getDatabaseName(), e.getMessage());
			return null;
		}
	}

	/**
	 * Creates a unique index for a field
	 * @param type
	 * @param field
	 * @return 
	 */
	public OIndex<?> createIndex(OType type, String field) {
		try {
			OrientVertexType vertexType = db.getTinkerpopInstance().getVertexType(className);
			if (vertexType == null) {
				vertexType = db.getTinkerpopInstance().createVertexType(className, "V");
			}

			vertexType.createProperty(field, type);
			return vertexType.createIndex(className + "." + field, OClass.INDEX_TYPE.UNIQUE, field);
		} catch (Exception e) {
			log.error("Could not create index {} on database {}. Reason is {}", type, getDatabaseName(), e.getMessage());
			return null;
		}
	}


	/**
	 * Returns the list of indices for the current orientdb class
	 * @return
	 */
	public Set<OIndex<?>> getIndexes() {
		try {
			OrientVertexType vertexType = db.getTinkerpopInstance().getVertexType(className);
			return vertexType.getIndexes();
		} catch (Exception e) {
			log.error("Could get indexes for database {} and class {} on database {}. Reason is {}", this.db.getTinkerpopInstance().getRawGraph().getName(), className, getDatabaseName(), e.getMessage());
			return null;
		}
	}

	/**
	 * Returns the database name where this schema belongs to
	 * @return
	 */
	private String getDatabaseName() {
		try {
			return this.db.getTinkerpopInstance().getRawGraph().getName();
		} catch (Exception e) {
			return "ERROR_GET_DATABASE_NAME";
		}
	}
}
