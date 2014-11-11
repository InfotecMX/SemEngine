/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package leveldb.test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.semanticwb.store.Graph;
import org.semanticwb.store.leveldb.GraphImp;

/**
 *
 * @author javier.solis.g
 */
public class loadDBPedia
{
    public static void main(String[] args) throws IOException
    {
        System.out.println("Init...");
        long time = System.currentTimeMillis();
        
        File dir=new File("/data/bench/2014");
       
        File files[]=dir.listFiles();
        for (File file : files) 
        {
            if(file.isFile() && !file.isHidden())
            {
                String name=file.getName();
                name=name.substring(0,name.indexOf("."));
                if(name.endsWith("_es"))name=name.substring(0,name.length()-3);
                //System.out.println(name);
                //addGraph(name, file);
                addGraph("dbpedia", file);
            }
        }
    }  
    
    public static void addGraph(String name, File file) throws IOException
    {
        long time = System.currentTimeMillis();
        System.out.println(file);
        
        HashMap<String,String> params=new HashMap();
        params.put("path", "/data/leveldb_dbp"); 
        
        Graph graph = new GraphImp(name,params);
        graph.setTransactionEnabled(false);          
        
        graph.load(file.getPath(),0,0);
        
        
        //System.out.println("Toral size "+name+":" + graph.count());
        
        graph.close();
        
        System.out.println("time load "+name+":" + (System.currentTimeMillis() - time));
    }
    
}
