package mem.test;

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
        
        
        Map params = new HashMap();
        params.put("path", "./demo");
        Graph tGraph = new GraphImp("HomePages", params);
        long time = System.currentTimeMillis();
        ((GraphImp)tGraph).createFromNT("/Data/FlatStoreDemoFiles/infoboxes-fixed.nt.gz");
        System.out.println("time: "+(System.currentTimeMillis()-time));
        
        
//        
//        File directory = new File("./demo");
//        FileTripleExtractor fte = new FileTripleExtractor(getFilename(directory, "HomePages", 1));
//        System.out.println("fte:"+fte.getCurrentTriple());
//        while (fte.getCurrentTriple()!=null) {
//            System.out.println("subj:"+fte.getCurrentTriple().getSubject());
//            fte.consumeCurrentTriple();
//        }
//        fte.close();
//        
        
    }
    
    private static File getFilename(File directory, String graphName, int part){
        String sPart = "00000"+part;
        return new File(directory, graphName+"_"+sPart.substring(sPart.length()-5));
    }
    
}
