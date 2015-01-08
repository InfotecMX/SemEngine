package flatstore.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.semanticwb.store.Graph;
import org.semanticwb.store.Triple;
import org.semanticwb.store.flatstore.GraphImp;
import org.semanticwb.store.flatstore.IdxBy;
import org.semanticwb.store.flatstore.TripleFileReader;

/**
 *
 * @author serch
 */
public class TripleComparator {

    /**
     * @param args the command line arguments
     */
/*    
    public static void main2(String[] args) {
        FileTripleExtractor ftp1 = new FileTripleExtractor("/Users/serch/Proyects/gitRepos/SemEngine/demo/HomePages.swbdb");
        FileTripleExtractor ftp2 = new FileTripleExtractor("/Users/serch/Proyects/gitRepos/SemEngine/demo1/HomePages.swbdb");
        long count = 0;
        while (ftp1.getCurrentTriple() != null) {
            count++;
            if (!getString(ftp1.getCurrentTriple()).equals(getString(ftp2.getCurrentTriple()))) {
                System.out.println("Triple: " + count);
                System.out.println("1: " + ftp1.getCurrentSubject() + " | " + ftp1.getCurrentProperty() + " | " + ftp1.getCurrentObject());
                System.out.println("1: " + ftp2.getCurrentSubject() + " | " + ftp2.getCurrentProperty() + " | " + ftp2.getCurrentObject());
            }
            ftp1.consumeCurrentTriple();
            ftp2.consumeCurrentTriple();
        }
        ftp1.close();
        ftp2.close();
    }
*/
 /*   
    
    public static void main3(String[] args) {
        FileTripleExtractor ftp1 = new FileTripleExtractor("/Users/serch/Proyects/gitRepos/SemEngine/demo/HomePages.swbdb");
        long count = 0;
        for (int x = 0; x < 200; x++) {
            count++;
            System.out.println("Triple: " + count);
            System.out.println("1: " + ftp1.getCurrentSubject() + " | " + ftp1.getCurrentProperty() + " | " + ftp1.getCurrentObject());
            ftp1.consumeCurrentTriple();
        }
        ftp1.close();
    }
    
    public static void main4(String[] args) {
        FileTripleExtractor ftp1 = new FileTripleExtractor("/Users/serch/Proyects/gitRepos/SemEngine/demo/HomePages-obj.swbdb");
        long count = 0;
        for (int x = 0; x < 200; x++) {
            count++;
            System.out.println("Triple: " + count);
            System.out.println("1: " + ftp1.getCurrentSubject() + " | " + ftp1.getCurrentProperty() + " | " + ftp1.getCurrentObject());
            ftp1.consumeCurrentTriple();
        }
        ftp1.close();
    }
    
    public static void main (String[] args) throws IOException {
        Map params = new HashMap();
        params.put("path", "./demo");
        Graph tGraph = new GraphImp("infoboxes", params);
        TripleFileReader tfr = new TripleFileReader("/Users/serch/Proyects/gitRepos/SemEngine/demo/infoboxes-sub", tGraph, IdxBy.SUBJECT);
        tfr.reset();
        Triple[] tg = tfr.getTripleGroup();
        System.out.println("Triple-1:"+tg.length+"- "+tg[0].toString());
        System.out.println("Triple-1:"+tg[tg.length-1].toString());
        tfr.next();
        tg = tfr.getTripleGroup();
        System.out.println("Triple-2:"+tg.length+"- "+tg[0].toString());
        System.out.println("Triple-2:"+tg[tg.length-1].toString());
        tfr.next();
        tg = tfr.getTripleGroup();
        System.out.println("Triple-3:"+tg.length+"- "+tg[0].toString());
        System.out.println("Triple-3:"+tg[tg.length-1].toString());
        tfr.next();
        tg = tfr.getTripleGroup();
        System.out.println("Triple-4:"+tg.length+"- "+tg[0].toString());
        System.out.println("Triple-4:"+tg[tg.length-1].toString());
        tfr.next();
        tg = tfr.getTripleGroup();
        System.out.println("Triple-5:"+tg.length+"- "+tg[0].toString());
        System.out.println("Triple-5:"+tg[tg.length-1].toString());
    }

//    static String getString(FakeTriple triple) {
//        return triple.getSubject() + "|" + triple.getProperty() + "|" + triple.getObject();
//    }
   */
}