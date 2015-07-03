package fs.orientdb;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import fs.orientdb.constants.CONFLICT_STRATEGY;

/**
 * Instance to Orientdb database that provides a pool of connections (transactional or non transactional) and access to some configurations such
 * as conflict strategies
 * @author alonsod86
 *
 */
public class ODatabase {

	// Instance to the OrientGraphFactory where the pool is located
	private OrientGraphFactory factory;

	// Instance to the graph interface of the wrapper that created this factory
	private GraphInterface graphInterface;

	public ODatabase(OrientGraphFactory factory, GraphInterface graphInterface) {
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

	/**
	 * Sets the conflict strategy when updating, inserting or deleting over old records
	 * @param strategy
	 */
	public void setConflictStrategy(CONFLICT_STRATEGY strategy) {
		// Parse strategy from enum to ALTER query
		String sStrategy = GraphInterface.CONFLIC_STRATEGY_VERSION;
		switch (strategy) {
		case CONTENT : sStrategy = GraphInterface.CONFLIC_STRATEGY_CONTENT; break;
		case AUTOMERGE : sStrategy = GraphInterface.CONFLIC_STRATEGY_AUTOMERGE; break;
		default:
			break;
		}
		
		// Alter conflict strategy for current database
		DB db = getDB();
		try {
			db.executeQuery("ALTER DATABASE CONFLICTSTRATEGY " + sStrategy);
			db.close();
		} catch (Exception e) {
			System.err.println("Could not alter conflictstrategy on database, reason " + e.getMessage());
			db.close();
		}
	}

	/**
	 * Returns the conflict strategy when updating, inserting or deleting over old records
	 * @return
	 */
	public String getConflictStrategy() {
		return factory.getDatabase().getStorage().getConfiguration().getConflictStrategy();
	}
}
