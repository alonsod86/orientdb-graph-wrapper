package fs.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Database instance with the interface to OrientDB
 * Created by dgutierrez on 23/5/15.
 */
public class DB {
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
                vertexType.createIndex(className + "." + pKey, OClass.INDEX_TYPE.UNIQUE, pKey);
            }
        }
        return (vertexType != null);
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
}
