package flatstore.test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.semanticwb.store.Graph;
import org.semanticwb.store.Triple;
import org.semanticwb.store.TripleIterator;
import org.semanticwb.store.flatstore.GraphImp;
import org.semanticwb.store.flatstore.PositionData;

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
        params.put("path", "/data/flatstore/");
        
        //Graph tGraph = new GraphImp("infoboxes", params);      
        GraphImp tGraph = new GraphImp("bench", params);
        
        //System.out.println("Count:"+tGraph.count());
        System.out.println("time: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        
        long idx=tGraph.subFileReader.getIdxLength()-1;
        System.out.println("idx:"+idx);
        
        String group=tGraph.subFileReader.getGroup(idx);
        System.out.println("group:"+group);
        System.out.println("find group: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        
        PositionData pdata=tGraph.subFileReader.findData("zz:vgrelease1:vgreleaseProperty", "zz:vgrelease1:vgreleaseProperty");
        System.out.println("get data: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        
        //pdata=pdata.back();
        //System.out.println("pdata back:"+pdata);
       
        
        for(int x=0;x<100 && pdata!=null;x++)
        {
            System.out.println(x+":"+" pdata:"+pdata.getPosition()+" ->"+pdata.getText());
            pdata=pdata.next();
        }
        
        System.out.println("");
        
        for(int x=0;x<110 && pdata!=null;x++)
        {
            System.out.println(x+":"+" pdata:"+pdata.getPosition()+" ->"+pdata.getText());
            pdata=pdata.back();
        }        
        
        
////        Iterator<Triple> it = tGraph.read2("/data/bench/SWBAdmin.nt", 0, 0);
////        while (it.hasNext()) {
////            Triple triple = it.next();
//            
//            TripleIterator it2=tGraph.findTriples(new Triple("<http://www.semanticwb.org/SWBAdmin#Resource:3>",null,null));
////            TripleIterator it2=tGraph.findTriples(new Triple("<http://www.semanticwb.org/SWBAdmin#Resource:3>","<http://www.semanticwebbuilder.org/swb4/ontology#views>","\"1310\"^^<http://www.w3.org/2001/XMLSchema#long>"));
//            //TripleIterator it2=tGraph.findTriples(triple);
//            while (it2.hasNext()) {
//                Triple triple2 = it2.next();
//                System.out.println("triple:"+triple2);
//            }            
////        }
        
        //System.out.println("it:"+it);
        
        System.out.println("walk: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        
        tGraph.close();
        
        System.out.println("time: "+(System.currentTimeMillis()-time));
    }
    
}
