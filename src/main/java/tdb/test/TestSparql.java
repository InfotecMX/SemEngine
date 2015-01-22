/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tdb.test;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.impl.ModelCom;
import com.hp.hpl.jena.tdb.TDBFactory;
import java.io.IOException;
import java.util.Iterator;
import org.semanticwb.store.jenaimp.SWBTSGraphDebug;

/**
 *
 * @author javier.solis.g
 */
public class TestSparql
{

    public static void main(String[] args) throws IOException
    {
        long time=System.currentTimeMillis();
        
        String directory = "/data/tdb" ;
        Dataset dataset = TDBFactory.createDataset(directory) ;
        //Model model = dataset.getDefaultModel();
        
        //Model model=new ModelCom(new SWBTSGraphDebug(dataset.getDefaultModel().getGraph()));
        Model model=new ModelCom(dataset.getDefaultModel().getGraph());
        
        //System.out.println("size:"+model.size());

        
        System.out.println("ini:"+(System.currentTimeMillis()-time));   
        time=System.currentTimeMillis();
        
//        System.out.println("size:" + model.size());
//
//        System.out.println("time size:" + (System.currentTimeMillis() - time));
//        time = System.currentTimeMillis();
        
        
        String query="SELECT * WHERE {\n" +
        "  <http://dbpedia.org/resource/Metropolitan_Museum_of_Art> ?p ?o\n" +
        "}";        
        query(query,model,false);        
        
        query="PREFIX p: <http://dbpedia.org/property/>\n" +
        "\n" +
        "SELECT ?film1 ?actor1 ?film2 ?actor2\n" +
        "WHERE {\n" +
        "  ?film1 p:starring <http://dbpedia.org/resource/Kevin_Bacon> .\n" +
        "  ?film1 p:starring ?actor1 .\n" +
        "  ?film2 p:starring ?actor1 .\n" +
        "  ?film2 p:starring ?actor2 .\n" +
        "}";        
        query(query,model);        
        
        query="PREFIX p: <http://dbpedia.org/property/>\n" +
        "\n" +
        "SELECT ?artist " +
                  "?artwork " +
                "?museum " +
                "?director\n" +
        "WHERE {\n" +
        "  ?artwork p:artist ?artist .\n" +
        "  ?artwork p:museum ?museum .\n" +
        "  ?museum p:director ?director\n" +
        "}";        
        query(query,model);  
        
        query="PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
        "\n" +
        "SELECT ?s ?homepage WHERE {\n" +
        "   <http://dbpedia.org/resource/Berlin> geo:lat ?berlinLat .\n" +
        "   <http://dbpedia.org/resource/Berlin> geo:long ?berlinLong . \n" +
        "   ?s geo:lat ?lat .\n" +
        "   ?s geo:long ?long .\n" +
        "   ?s foaf:homepage ?homepage .\n" +
        "   FILTER (\n" +
        "     ?lat        <=     ?berlinLat + 0.03190235436 &&\n" +
        "     ?long       >=     ?berlinLong - 0.08679199218 &&\n" +
        "     ?lat        >=     ?berlinLat - 0.03190235436 && \n" +
        "     ?long       <=     ?berlinLong + 0.08679199218)\n" +
        "}";        
        query(query,model,false);  
        
        query="PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
        "PREFIX p: <http://dbpedia.org/property/>\n" +
        "\n" +
        "SELECT * WHERE {\n" +
        "   <http://dbpedia.org/resource/New_York_City> geo:lat ?nyLat .\n" +
        "   <http://dbpedia.org/resource/New_York_City> geo:long ?nyLong . \n" +
        "   ?s geo:lat ?lat .\n" +
        "   ?s geo:long ?long .\n" +
        "   ?s p:architect ?a .\n" +
        "   ?a foaf:homepage ?homepage .\n" +
        "   FILTER (\n" +
        "     ?lat        <=     ?nyLat + 0.3190235436 &&\n" +
        "     ?long       >=     ?nyLong - 0.8679199218 &&\n" +
        "     ?lat        >=     ?nyLat - 0.3190235436 && \n" +
        "     ?long       <=     ?nyLong + 0.8679199218)\n" +
        "}";        
        query(query,model,false);          
        
        model.close();
        
        dataset.close();
        
        System.out.println("end:"+(System.currentTimeMillis()-time));   
        time=System.currentTimeMillis();

    }
    
    public static void query(String query, Model model)
    {
        query(query, model, false);
    }    
    
    public static void query(String query, Model model, boolean print)
    {
        long time=System.currentTimeMillis();        
        QueryExecution qe=null;//site.getSemanticModel().sparQLQuery(query);
        
        Query q = QueryFactory.create(query, Syntax.syntaxSPARQL_11);
        qe = QueryExecutionFactory.create(q, model);
        
        ResultSet rs=qe.execSelect();
        {
            Iterator<String> it=rs.getResultVars().iterator();
            while(it.hasNext())
            {
                String name=it.next();
                System.out.print(name);
                System.out.print('\t');
            }
        }
        System.out.println();
        int x=0;
        while(rs.hasNext())
        {
            QuerySolution qs=rs.next();
            if(print)
            {
            Iterator<String> it=rs.getResultVars().iterator();
            while(it.hasNext())
            {
                String name=it.next();
                RDFNode node=qs.get(name);
                String val="";
                if(node!=null&&node.isLiteral())val=node.asLiteral().getLexicalForm();
                else if(node!=null&&node.isResource())val=node.asResource().getURI();
                System.out.print(val);
                System.out.print('\t');
            }
            System.out.println();
            }
            x++;
        }
        System.out.println("query:"+x+" "+(System.currentTimeMillis()-time));   
        time=System.currentTimeMillis();        
    }
}
