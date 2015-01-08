package flatstore.test;

import java.io.File;
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
        params.put("path", "/data/SWBAdmin/");
        
        Graph tGraph = new GraphImp("SWBAdmin", params);
        ((GraphImp)tGraph).createFromNT("/data/bench/SWBAdmin.nt");
        
//        Graph tGraph = new GraphImp("infoboxes", params);
//        ((GraphImp)tGraph).createFromNT("/data/bench/infoboxes-fixed.nt.gz");
        
//        Graph tGraph = new GraphImp("DBMS100M", params);
//        ((GraphImp)tGraph).createFromNT("/data/bench/dataset_100m.nt.gz");
        
        System.out.println("time: "+(System.currentTimeMillis()-time));
                   
//        File directory = new File("./demo");
//        FileTripleExtractor fte = new FileTripleExtractor(getFilename(directory, "HomePages", 1));
//        System.out.println("fte:"+fte.getCurrentTriple());
//        while (fte.getCurrentTriple()!=null) {
//            System.out.println("subj:"+fte.getCurrentTriple().getSubject());
//            fte.consumeCurrentTriple();
//        }
//        fte.close();
//        
        tGraph.close();
    }
    
    private static File getFilename(File directory, String graphName, int part){
        String sPart = "00000"+part;
        return new File(directory, graphName+"_"+sPart.substring(sPart.length()-5));
    }
    
}
