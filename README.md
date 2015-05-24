# orientdb-graph-wrapper
This is an open source Java wrapper for OrientDB graph database. This library is intented to simplify common graph operations like searching nodes or relationships, indexing fields and working with schemas.

orientdb-graph-wrapper works by default using a configurable connection pool, allowing concurrent connections on high throughput systems.

## Usage
Starting with orientdb-graph-wrapper is really straightforward
```Java
OrientConfiguration config = new OrientConfiguration("my_database","my_schema",1,10,OrientConfiguration.DATABASE_MEMORY);
GraphInterface g = new GraphInterface(config);
```
Now we have a connection to our graph database. If the database and schema did not exist before, the wrapper will create it for you.

### Connection
There are three ways of using The graph interface
* **In memory**. Ideal for testing.
* Using a **local directory**. If you don't want to use a remote server this will create every directory and file for you.
* Using a **remote server**. If you want to connect to your remote OrientDB server.

Using the wrapper and the OrientConfiguration is this simple
```Java
// In memory database
new OrientConfiguration("in_memory","my_schema", 1, 10, OrientConfiguration.DATABASE_MEMORY);
// Local database
new OrientConfiguration("/dir/mydb", "my_schema", 1, 10, OrientConfiguration.DATABASE_LOCAL);
// Remote database
new OrientConfiguration("localhost","my_schema", 1, 10, "root", "root", OrientConfiguration.DATABASE_REMOTE);
````
### Working with databases
This wrapper separates the database from the collections (classes in OrientDB). Every database connection is managed by the pool. To get a database connection do this
```Java
GraphInterface g = new GraphInterface(config);
DB graphNoTx = g.getDB();
DB graphTx = g.getDB(true);
```
With this simple command we have two instances to the database, a simple one and a transactional one. After doing every operation with your database **remember to close the connection** to return the database to the pool.
```Java
// closing one by one
graphNoTx.close();
// closing all
g.closeAll();
```
### Working with collections
Once we have a connection with our database we need to access a specific collection or class in order to start inserting, querying or deleting data. To retreive a collection instance do this
```Java
Schema sc = db.getSchema("my_schema");
```
Now we have access to every simplified operation within the database. Let's see a brief example
```Java
Vertex v1 = sc.createNode(new Pk("My_Pk", 1));
Vertex v2 = sc.createNode(new Pk("My_Pk", 2));
sc.createRelation(v1, v2, "Relation_Name");
```
There are many wrapped operations. Check the UnitTest class for further information.
