import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;

import fs.orientdb.DB;
import fs.orientdb.GraphInterface;
import fs.orientdb.ODatabase;
import fs.orientdb.OrientConfiguration;
import fs.orientdb.Pk;
import fs.orientdb.Schema;

/**
 * Created by tiocansino on 23/5/15.
 */

public class UnitTests {

    private OrientConfiguration config;
    private GraphInterface g;

    private String TEST_CLASS = "test";
    private String TEST_INDEX = "index";
    private String TEST_PKEY = "pkey";
    private String TEST_RELATION = "relation";

    @Before
    public void initConfig() throws IOException {
        this.config = new OrientConfiguration("in_memory",1,1,"admin","admin", OrientConfiguration.DATABASE_MEMORY);
        //this.config = new OrientConfiguration("/dir/mydb", "my_schema", 1, 10, OrientConfiguration.DATABASE_LOCAL);
        //this.config = new OrientConfiguration("localhost","test",1,1,"root", "root", OrientConfiguration.DATABASE_REMOTE);
        g = new GraphInterface(this.config);
        g.createDatabase("my_database");
    }
    
    @After
    public void clear() throws IOException {
    	g.dropDatabase("my_database");
    }

    @Test
    public void testNoTx() throws IOException {
        DB graph = g.getOFactory("my_database").getDB();
        Assert.assertTrue(!graph.isTransactional());
    }

    @Test
    public void testTx() throws IOException {
        DB graph = g.getOFactory("my_database").getDB(true);
        Assert.assertTrue(graph.isTransactional());
    }

    @Test
    public void testPool() throws IOException {
    	ODatabase factory = g.getOFactory("my_database");
        int created = factory.getFactory().getCreatedInstancesInPool();
        factory.getDB();
        int available = factory.getFactory().getAvailableInstancesInPool();
        Assert.assertTrue(available == (created-1));
    }

    @Test
    public void testExistClass() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Assert.assertTrue(!db.existClass(TEST_CLASS));
        Assert.assertTrue(!db.existClass(TEST_CLASS, TEST_PKEY));
        Assert.assertTrue(db.existClass(TEST_CLASS, TEST_PKEY, true));
    }

    @Test
    public void testExistNode() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        // create index for searching
        //sc.createIndex(OType.INTEGER, TEST_PKEY);
        sc.createNode(new Pk(TEST_PKEY, 1));
        Assert.assertTrue(sc.existNode(new Pk(TEST_PKEY, 1))!=null);
    }

    @Test
    public void testCreateIndex() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_INDEX);
        sc.createIndex(OType.STRING, TEST_PKEY);
        Assert.assertEquals(sc.getIndexes().iterator().next().getName(), TEST_INDEX + "." + TEST_PKEY);
    }

    @Test
    public void testNodeChanges() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        //sc.createIndex(OType.INTEGER, TEST_PKEY);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("attrib1", "val1");
        attributes.put("attrib2", "val2");

        Vertex v = sc.createNode(new Pk(TEST_PKEY, 1), attributes);
        Assert.assertTrue(!sc.nodeHasChanged(v, attributes));

        attributes.put("attrib3", "val3");
        Assert.assertTrue(sc.nodeHasChanged(v, attributes));
    }

    @Test
    public void testUpdateNode() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        sc.createIndex(OType.INTEGER, TEST_PKEY);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("attrib1", "val1");
        attributes.put("attrib2", "val2");

        Vertex v = sc.createNode(new Pk(TEST_PKEY, 1), attributes);
        attributes.put("attrib3", "val3");
        sc.updateNode(v, attributes);

        v = sc.existNode(new Pk(TEST_PKEY, 1));
        Assert.assertTrue(v.getPropertyKeys().size()==4);
    }

    @Test
    public void testExistRelationClass() throws IOException {
        DB sc = g.getOFactory("my_database").getDB();
        Assert.assertTrue(!sc.existRelationClass(TEST_RELATION));
        Assert.assertTrue(sc.existRelationClass(TEST_RELATION, true));
    }

    @Test
    public void testCreateRelation() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        Vertex v1 = sc.createNode(new Pk(TEST_PKEY, 1));
        Vertex v2 = sc.createNode(new Pk(TEST_PKEY, 2));
        db.createRelation(v1, v2, TEST_RELATION);

        Assert.assertTrue(db.getTinkerpopInstance().countEdges()==1);
    }

    @Test
    public void testRelationHasChanged() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        Vertex v1 = sc.createNode(new Pk(TEST_PKEY, 1));
        Vertex v2 = sc.createNode(new Pk(TEST_PKEY, 2));
        Edge edge = db.createRelation(v1, v2, TEST_RELATION);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        Assert.assertTrue(!db.relationHasChanged(edge, attributes));
        attributes.put("attrib1", "val1");
        Assert.assertTrue(db.relationHasChanged(edge, attributes));
    }

    @Test
    public void testRelationUpdate() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        Vertex v1 = sc.createNode(new Pk(TEST_PKEY, 1));
        Vertex v2 = sc.createNode(new Pk(TEST_PKEY, 2));
        HashMap<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("attrib1", "val1");
        Edge edge = db.createRelation(v1, v2, TEST_RELATION, attributes);
        Assert.assertTrue(edge.getPropertyKeys().size()==1);
        attributes.put("attrib2", "val2");
        edge = db.relationUpdate(edge, attributes);
        Assert.assertTrue(edge.getPropertyKeys().size()==2);
    }

    @Test
    public void testExecuteQuery() throws Exception {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        Vertex v1 = sc.createNode(new Pk(TEST_PKEY, 1));
        Vertex v2 = sc.createNode(new Pk(TEST_PKEY, 2));
        db.createRelation(v1, v2, TEST_RELATION);

        OrientDynaElementIterable query = db.executeQuery("SELECT FROM E");
        Iterator<Object> it = query.iterator();
        // only one relation
        it.next();
        Assert.assertTrue(!it.hasNext());

        query = db.executeQuery("SELECT FROM V");
        it = query.iterator();
        // two vertices
        it.next();
        it.next();
        Assert.assertTrue(!it.hasNext());
    }

    @Test
    public void testNodesRelated() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        Vertex v1 = sc.createNode(new Pk(TEST_PKEY, 1));
        Vertex v2 = sc.createNode(new Pk(TEST_PKEY, 2));
        db.createRelation(v1, v2, TEST_RELATION);
        Iterator<Vertex> it = sc.getNodesRelated(v1, Direction.BOTH).iterator();
        // only one vertex
        Vertex e = it.next();
        Assert.assertTrue(e.getId().equals(v2.getId()));
        // test it with name
        it = sc.getNodesRelated(v1, Direction.BOTH, TEST_RELATION).iterator();
        e = it.next();
        Assert.assertTrue(e.getId().equals(v2.getId()));
    }

    @Test
    public void testGetVertexRelations() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        Vertex v1 = sc.createNode(new Pk(TEST_PKEY, 1));
        Vertex v2 = sc.createNode(new Pk(TEST_PKEY, 2));
        Edge edge = db.createRelation(v1, v2, TEST_RELATION);
        Iterator<Edge> it = sc.getRelations(v2, Direction.OUT).iterator();
        Edge e = it.next();
        Assert.assertTrue(e.getId().equals(edge.getId()));

        it = sc.getRelations(v1, Direction.IN).iterator();
        e = it.next();
        Assert.assertTrue(e.getId().equals(edge.getId()));
    }

    @Test
    public void testRelationNames() throws IOException {
        DB db = g.getOFactory("my_database").getDB();
        Schema sc = db.getSchema(TEST_CLASS);
        Vertex v1 = sc.createNode(new Pk(TEST_PKEY, 1));
        Vertex v2 = sc.createNode(new Pk(TEST_PKEY, 2));
        db.createRelation(v1, v2, TEST_RELATION);
        db.createRelation(v1, v2, TEST_RELATION + "_2");

        List<String> names = sc.getRelationsNames(v1,Direction.IN);

        Assert.assertTrue(names.size()==2);
        Assert.assertTrue(names.get(0).equals(TEST_RELATION));
        Assert.assertTrue(names.get(1).equals(TEST_RELATION + "_2"));
    }
    
    public void testConflictStrategy() {
    	// TODO: check creation of database with different conflict strategies
    }
}
