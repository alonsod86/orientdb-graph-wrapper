package fs.orientdb;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

/**
 * Wrapper for OrientdbGraphFactory. This class is responsible for retrieving database instances using the configuration
 * of the given factory
 * @author alonsod86
 *
 */
public class OFactory {

	// Instance to the OrientGraphFactory where the pool is located
	private OrientGraphFactory factory;
	
	// Instance to the graph interface of the wrapper that created this factory
	private GraphInterface graphInterface;
	
	public OFactory(OrientGraphFactory factory, GraphInterface graphInterface) {
		this.factory = factory;
		this.graphInterface = graphInterface;
	}
	
	public DB getDB() {
		return new DB(factory, false);
	}
	
	public DB getDB(boolean transactional) {
		return new DB(factory, transactional);
	}
	
	public OrientGraphFactory getFactory() {
		return factory;
	}
	
	public GraphInterface getGraphInterface() {
		return graphInterface;
	}
}
