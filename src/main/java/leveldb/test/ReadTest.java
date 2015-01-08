package leveldb.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.semanticwb.store.Graph;
import org.semanticwb.store.Triple;
import org.semanticwb.store.TripleIterator;
import org.semanticwb.store.leveldb.GraphImp;

/**
 *
 * @author serch
 */
public class ReadTest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        
        long time = System.currentTimeMillis();        
        Map params = new HashMap();
        params.put("path", "/data/leveldb/");
        
        //Graph tGraph = new GraphImp("infoboxes", params);      
        Graph tGraph = new GraphImp("swb", params);
        
        System.out.println("Count:"+tGraph.count());
        System.out.println("time: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        
        
        Iterator<Triple> it = tGraph.read2("/data/bench/SWBAdmin.nt", 0, 0);
        while (it.hasNext()) {
            Triple triple = it.next();
            
            //TripleIterator it2=tGraph.findTriples(new Triple("<http://www.semanticwb.org/SWBAdmin#Resource:3>","<http://www.semanticwebbuilder.org/swb4/ontology#views>","\"1310\"^^<http://www.w3.org/2001/XMLSchema#long>"));
            TripleIterator it2=tGraph.findTriples(triple);
            while (it2.hasNext()) {
                Triple triple2 = it2.next();
                //System.out.println("triple:"+triple2);
            }            
        }
        
        //System.out.println("it:"+it);
        
        System.out.println("time: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        
        tGraph.close();
        
        System.out.println("time: "+(System.currentTimeMillis()-time));
    }
    
}
