/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package leveldb.test;

import java.io.IOException;
import java.util.HashMap;
import org.semanticwb.store.Graph;
import org.semanticwb.store.Triple;
import org.semanticwb.store.TripleIterator;
import org.semanticwb.store.leveldb.GraphImp;

/**
 *
 * @author javiersolis
 */
public class nativeDBPedia {
 public static void main(String[] args) throws IOException
    {
        System.out.println("Init...");
        long time = System.currentTimeMillis();
        
        HashMap<String,String> params=new HashMap();
        params.put("path", "/data/leveldb_dbp");
                
        Graph graph = new GraphImp("dbpedia",params);
        graph.setTransactionEnabled(false);
        //graph.setEncodeURIs(false);

        System.out.println("time db:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();
        
        try
        {
            long c=graph.count();
            System.out.println("size:" + c);
            
        }catch(Exception e)
        {
            e.printStackTrace();
        }finally
        {
            time = System.currentTimeMillis();
            graph.close();
            System.out.println("time fin:" + (System.currentTimeMillis() - time));
        }        
    }        
}
