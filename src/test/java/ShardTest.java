
import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import fs.orientdb.GraphInterface;
import fs.orientdb.ODatabase;
import fs.orientdb.OrientConfiguration;


public class ShardTest {

	@Test
	public void insertTest() throws Exception {
		OrientConfiguration config = new OrientConfiguration("localhost:2424;localhost:2425",1,1,"root", "toor", OrientConfiguration.DATABASE_REMOTE);
		GraphInterface g = new GraphInterface(config);
		System.out.println(g.getOFactory("GratefulDeadConcerts").getDB().existClass("V"));
		System.out.println(g.getOFactory("GratefulDeadConcerts").getDB().existClass("V"));
		System.out.println(g.getOFactory("GratefulDeadConcerts").getDB().existClass("V"));
	}
	
	@Ignore
	@Test
	public void queryTest() throws IOException {
		OrientConfiguration config = new OrientConfiguration("localhost",1,1,"root", "toor", OrientConfiguration.DATABASE_REMOTE);
		GraphInterface g = new GraphInterface(config);
		
		ODatabase factory = g.getOFactory("GratefulDeadConcerts");
		boolean res = factory.getDB().existClass("V");
		g.getOServer().listDatabases();
		System.out.println(res);
	}
}
