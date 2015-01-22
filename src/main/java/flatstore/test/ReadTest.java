package flatstore.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.semanticwb.store.Triple;
import org.semanticwb.store.TripleIterator;
import org.semanticwb.store.flatstore.GraphImp;

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
        
        System.out.println("Count:"+tGraph.count());
        System.out.println("time: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
/*        
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
        System.out.println("walk: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
*/        
        
        TripleIterator it=tGraph.findTriples(new Triple("<http://dbpedia.org/resource/Giants_Stadium>","<http://dbpedia.org/property/architect>",null));
        //TripleIterator it=tGraph.findTriples(new Triple("<http://dbpedia.org/resource/Giants_Stadium>",null,null));
        while (it.hasNext()) {
            Triple triple = it.next();
            System.out.println("triple:"+triple);
        }    
        
        System.out.println("find: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        
        //System.out.println("it:"+it);
        
        
        tGraph.close();
        
        System.out.println("time: "+(System.currentTimeMillis()-time));
    }
    
}
