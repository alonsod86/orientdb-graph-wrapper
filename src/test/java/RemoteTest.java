import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.Vertex;

import fs.orientdb.Schema;
import fs.orientdb.DB;
import fs.orientdb.GraphInterface;
import fs.orientdb.OrientConfiguration;
import fs.orientdb.Pk;


public class RemoteTest {

    private OrientConfiguration config;
    private GraphInterface g;
    private String TEST_CLASS = "test";
    private String TEST_PKEY = "pkey";
    private String TEST_RELATION = "relation";
    @Before
    public void initConfig() throws IOException {
        this.config = new OrientConfiguration("192.168.10.208",1,1,"root", "toor", OrientConfiguration.DATABASE_REMOTE);
        g = new GraphInterface(this.config);

    }
    
    @Test
    public void testClass() throws IOException {
//    	DB db = g.getDB();
//    	db.createClass("PRUEBAS", "primaryKey");
//    	Collection c = db.getSchema("PRUEBAS");
//    	//for (int i=0; i<1000000; i++)
//    	c.createNode(new Pk("primaryKey", System.currentTimeMillis()));
    	
    	DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        Vertex v1 = sc.createNode(new Pk(TEST_PKEY, 1));
        Vertex v2 = sc.createNode(new Pk(TEST_PKEY, 2));
        db.createRelation(v1, v2, TEST_RELATION);

        Assert.assertTrue(db.getGraphEngine().countEdges()==1);
    }
}
