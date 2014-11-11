/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package leveldb.test;

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
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.semanticwb.store.jenaimp.SWBTSGraph;
import org.semanticwb.store.leveldb.GraphImp;

/**
 *
 * @author javiersolis
 */
public class searchDBPedia 
{

    public static void main(String[] args) throws IOException
    {
        System.out.println("Init...");
        long time=System.currentTimeMillis();
        
        HashMap<String,String> params=new HashMap();
        params.put("path", "/data/leveldb_dbp");
        
        Model model=new ModelCom(new SWBTSGraph(new GraphImp("dbpedia",params)));
        
        System.out.println("ini:"+(System.currentTimeMillis()-time));   
        time=System.currentTimeMillis();
        
        String query="select * where {<http://es.wikipedia.org/wiki/(10160)_Totoro> ?p ?o}";
        
        query(query,model);        
        
        query(query,model);  
        
        query="select * where {?s <http://xmlns.com/foaf/0.1/primaryTopic> ?o}";
        
        query(query,model);      
        
        model.close();
        
        System.out.println("end:"+(System.currentTimeMillis()-time));   
    }
    
    public static void query(String query, Model model)
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
            if(x<500)
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
