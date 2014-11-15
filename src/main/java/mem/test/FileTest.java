/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mem.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.semanticwb.store.FlatStoreFile;
import org.semanticwb.store.Graph;
import org.semanticwb.store.Triple;
import org.semanticwb.store.leveldb.GraphImp;

/**
 *
 * @author serch
 */
public class FileTest {

    /**
     * @param args the command line arguments
     */
//    public static void main(String[] args) throws IOException {
//        Map params = new HashMap();
//        params.put("path", ".");
//        Graph tGraph = new GraphImp("SWBAdmin", params);
//        Iterator<Triple> it = tGraph.read("/Users/serch/Proyects/aspects/build/web/swbadmin/rdf/SWBAdmin.nt", 0, 0);
//        FlatStoreFile fff = FlatStoreFile.create("./fffdemo01", tGraph);
//        
//        while (it.hasNext()){
//            System.out.println("pos:"+fff.add(it.next()));
//        }
//        fff.close();
//    }
    
    public static void main(String[] args) throws IOException {
        Map params = new HashMap();
        params.put("path", ".");
        Graph tgraph=new GraphImp("SWBAdmin", params);
        FlatStoreFile fff = FlatStoreFile.open("./fffdemo01", tgraph);
        Triple t;
        long pos = 0;
//        t = fff.readTriple(0);
//        System.out.println("T0: "+t);
//        t = fff.readTriple(405820);
//        System.out.println("T1: "+t);
//        t = fff.readTriple(1910);
//        System.out.println("T2: "+t);
//        t = fff.readTriple(15040);
//        System.out.println("T3: "+t);
//        t = fff.readTriple(41580);
//        System.out.println("T4: "+t);
//        t = fff.readTriple(403870);
//        System.out.println("T5: "+t);
//        do {
//            System.out.println("triple:"+fff.readTriple(pos));
//            pos = fff.getNextTriplePosition(pos);
//        } while (pos>-1);
        pos = fff.getPreviousTriplePosition(fff.getSize());
        do {
            System.out.println("triple:"+fff.readTriple(pos));
            pos = fff.getPreviousTriplePosition(pos);
        } while (pos > -1);
    }
}
