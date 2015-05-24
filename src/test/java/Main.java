/**
 * {@code Main}.
 *
 * @author <a href="mailto:rmuller@xiam.nl">Ronald K. Muller</a>
 */
public class Main {
//    public static void main(String... args) {
//        try {
//            OrientDBInterface odbi = new OrientDBInterface("remote:localhost", "Probando", "root", "root");
//            OrientGraphNoTx g = odbi.getGraphDB();
//
//            /*Vertex vertex = odbi.existNode("person", "1478268520");
//            Iterable<Vertex> iVertex = odbi.getNodesRelated(vertex, 1);
//
//            Iterable<Edge> iEdges = odbi.getRelations(vertex, 1);
//            List<String> lEdges = odbi.getRelationsNames(vertex, 1);*/
//
//            List<Node> lNodos = new ArrayList<Node>();
//            Hashtable<String,String> att = new Hashtable<String, String>();
//            att.put("alias", "yolanda.cuevas.5496");
//            att.put("name", "Yolanda Cuevas");
//            att.put("urlProfile", "https://www.facebook.com/yolanda.cuevas.5496");
//            Node nodo = new Node("person", "1478268520", att);
//            lNodos.add(nodo);
//
//            Hashtable<String,String> att2 = new Hashtable<String,String>();
//            att2.put("alias", "yolanda.cuevas.5496");
//            att2.put("name", "Vita da Mamma");
//            att2.put("urlProfile", "https://www.facebook.com/pages/Vita-da-Mamma/123553327675696");
//            Node nodo2 = new Node("person", "123553327675696", att2);
//            lNodos.add(nodo2);
//            Hashtable<String,String> att3 = new Hashtable<String,String>();
//            att3.put("alias", "100008814139457");
//            att3.put("name", "Pilar Sanchez");
//            att3.put("urlProfile", "https://www.facebook.com/profile.php?id=100008814139457&fref=pb&hc_location=profile_browser");
//            Node nodo3 = new Node("person", "100008814139457", att3);
//            lNodos.add(nodo3);
//
//
//
//            // database is now auto created
//            // Changed to be compatible with current OrientDB implementation
//            for (Node nn : lNodos){
//                Vertex nodus = null;
//                try{
//                    nodus = odbi.existNode("person", nn.getId());
//                }catch (IllegalArgumentException iae){
//                    //nada, no existe... pero los jod�os lanzan una excepci�n en lugar de devolver null!!!!!
//                    //Si no existe, nulll, pero si lo que no existe es el index, cascot�n.
//                }
//                if (nodus == null){
//                    OrientVertexType vertexType = g.getVertexType(nn.getClassName());
//                    if (vertexType == null){
//                        vertexType = g.createVertexType(nn.getClassName(), "V");
//                        vertexType.createProperty("idPerson", OType.STRING);
//                        vertexType.createIndex(nn.getClassName() + "." + nn.getClassName() + "_id", OClass.INDEX_TYPE.UNIQUE, "idPerson");
//                    }
//                    nodus = g.addVertex("class:" + nn.getClassName());
//                    nodus.setProperty("idPerson", nn.getId());
//                    Iterator<String> itr = nn.getAttributes().keySet().iterator();
//                    while (itr.hasNext()) {
//                        String key = itr.next();
//                        nodus.setProperty(key, nn.getAttributes().get(key));
//                    }
//                }
//            }
//
//            List<Relation> lRels = new ArrayList<Relation>();
//            Relation relation = new Relation("relates", "100008814139457", "1478268520", null);
//            relation.setClassNameIn("person");
//            relation.setClassNameOut("person");
//            lRels.add(relation);
//            relation = new Relation("relates", "123553327675696", "1478268520", null);
//            relation.setClassNameIn("person");
//            relation.setClassNameOut("person");
//            lRels.add(relation);
//            relation = new Relation("relates", "123553327675696", "100008814139457", null);
//            relation.setClassNameIn("person");
//            relation.setClassNameOut("person");
//            lRels.add(relation);
//
//            for (Relation rel : lRels){
//                Vertex nodusIn = null;
//                Vertex nodusOut = null;
//                try{
//                    nodusIn = g.getVertexByKey( rel.getClassNameIn() + "." + rel.getClassNameIn() + "_id", rel.getIn());
//                    nodusOut = g.getVertexByKey( rel.getClassNameOut() + "." + rel.getClassNameOut() + "_id", rel.getOut());
//                }catch (IllegalArgumentException iae){
//                    //nada, no existe... pero los jod�os lanzan una excepci�n en lugar de devolver null!!!!!
//                    //Si no existe, nulll, pero si lo que no existe es el index, cascot�n.
//                }
//                if (nodusIn != null && nodusOut != null){
//                    if (odbi.existRelationClass("relates", true)){
//                        Edge edge = odbi.existRelation(nodusIn, nodusOut, rel.getName(), true, null);
//                        if (edge != null){
//                            if (rel.getAttributes() != null){
//                                Iterator<String> itr = rel.getAttributes().keySet().iterator();
//                                while (itr.hasNext()) {
//                                    String key = itr.next();
//                                    edge.setProperty(key, rel.getAttributes().get(key));
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        } finally {
//            // this also closes the OrientGraph instances created by the factory
//            // Note that OrientGraphFactory does not implement Closeable
//
//        }
//    }

}