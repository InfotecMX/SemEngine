/*
 * To change this template, choose Tools | Templates
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
 * @author javier.solis.g
 */
public class BSBMLoad
{
    public static void main(String[] args) throws IOException
    {
        System.out.println("Init...");
        long time = System.currentTimeMillis();
        
        HashMap<String,String> params=new HashMap();
        params.put("path", "/data/leveldb");
                
        Graph graph = new GraphImp("bsbm",params);
        graph.setTransactionEnabled(false);
        //graph.setEncodeURIs(false);

        System.out.println("time db:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();
        
        try
        {
            long c=graph.count();
            System.out.println("size:" + c);

            System.out.println("time size:" + (System.currentTimeMillis() - time));
            time = System.currentTimeMillis();

    //************* TEST  

            if (c == 0)
            {
                try
                {
                    graph.load("/data/bench/dataset_100m.nt.gz",0,0);
                } catch (Exception e)
                {
                    e.printStackTrace();
                }

                System.out.println("time bsbm:" + (System.currentTimeMillis() - time));
                time = System.currentTimeMillis();       
                
                
                //System.out.println(graph.count());          
                //System.out.println("time size:" + (System.currentTimeMillis() - time));
                //time = System.currentTimeMillis();                                
            }

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
