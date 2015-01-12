package flatstore.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.semanticwb.store.Graph;
import org.semanticwb.store.flatstore.GraphImp;

/**
 *
 * @author serch
 */
public class StorageTest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        
        long time = System.currentTimeMillis();        
        Map params = new HashMap();
        params.put("path", "/data/flatstore/");
        
//        Graph tGraph = new GraphImp("SWBAdmin", params);
//        ((GraphImp)tGraph).createFromNT("/data/bench/SWBAdmin.nt");
        
        Graph tGraph = new GraphImp("bench", params);
        ((GraphImp)tGraph).createFromNT("/data/bench/infoboxes-fixed.nt.gz","/data/bench/geocoordinates-fixed.nt.gz","/data/bench/homepages-fixed.nt.gz");
        
//        Graph tGraph = new GraphImp("DBMS100M", params);
//        ((GraphImp)tGraph).createFromNT("/data/bench/dataset_100m.nt.gz");
        
        System.out.println("time: "+(System.currentTimeMillis()-time));
                   
        tGraph.close();
    }
    
}
