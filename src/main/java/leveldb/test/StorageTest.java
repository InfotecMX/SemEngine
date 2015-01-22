package leveldb.test;

import java.io.IOException;
import java.util.HashMap;
import org.semanticwb.store.Triple;
import org.semanticwb.store.TripleIterator;
import org.semanticwb.store.leveldb.GraphImp;

/**
 *
 * @author serch
 */
public class StorageTest {

    public static void main(String[] args) throws IOException {
        System.out.println("Init...");
        long time = System.currentTimeMillis();

        HashMap<String, String> params = new HashMap();
        params.put("path", "/data/leveldb");

        org.semanticwb.store.Graph graph = new GraphImp("swb", params);
        graph.setTransactionEnabled(false);
        //graph.setEncodeURIs(false);

        System.out.println("time db:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();

        try {
            graph.load("/data/bench/infoboxes-fixed.nt.gz", 0, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("time infoboxes:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();

        try {
            graph.load("/data/bench/geocoordinates-fixed.nt.gz", 0, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("time geocoordinates:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();

        try {
            graph.load("/data/bench/homepages-fixed.nt.gz", 0, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("time homepages:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();
        
        graph.close();

    }
}
