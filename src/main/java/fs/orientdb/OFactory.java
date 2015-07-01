package fs.orientdb;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

/**
 * Wrapper for OrientdbGraphFactory. This class is responsible for retrieving database instances using the configuration
 * of the given factory
 * @author alonsod86
 *
 */
public class OFactory {

	private OrientGraphFactory factory;
	
	public OFactory(OrientGraphFactory factory) {
		this.factory = factory;
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
}
