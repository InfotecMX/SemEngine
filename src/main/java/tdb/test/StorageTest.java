package tdb.test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileUtils;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;


/**
 *
 * @author serch
 */
public class StorageTest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Init...");
        long time = System.currentTimeMillis();
        
        String directory = "/data/tdb" ;
        Dataset dataset = TDBFactory.createDataset(directory) ;
        Model model = dataset.getDefaultModel();
        System.out.println("geocoordinates");
        dataset.begin(ReadWrite.WRITE);
        model.read(new GZIPInputStream(new FileInputStream("/data/bench/geocoordinates-fixed.nt.gz")), null, FileUtils.langNTriple);
        dataset.end();
        System.out.println("time: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        System.out.println("homepages");
        dataset.begin(ReadWrite.WRITE);
        model.read(new GZIPInputStream(new FileInputStream("/data/bench/homepages-fixed.nt.gz")), null, FileUtils.langNTriple);
        dataset.end() ;
        System.out.println("time: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();
        System.out.println("infoboxes");
        dataset.begin(ReadWrite.WRITE);
        model.read(new GZIPInputStream(new FileInputStream("/data/bench/infoboxes-fixed.nt.gz")), null, FileUtils.langNTriple);
        dataset.end() ;
        dataset.close();
        System.out.println("time: "+(System.currentTimeMillis()-time));time=System.currentTimeMillis();     
    }    
}
