package org.semanticwb.store.flatstore;

import org.semanticwb.store.TripleWrapper;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.Comparator;

/**
 *
 * @author serch
 */
public class WriterTask implements Callable<String>
{
    public static volatile int intances=0;
    
    private final String baseFilename;
    final private List<TripleWrapper> listS;
    final private List<TripleWrapper> listP;
    final private List<TripleWrapper> listO;
    private OutputStream out;
    
    
//    private final Function<TripleWrapper, String> bySubject = TripleWrapper::getSubject;
//    private final Function<TripleWrapper, String> byProp = TripleWrapper::getProperty;
//    private final Function<TripleWrapper, String> byObj = TripleWrapper::getObject;
    
//    private Comparator<TripleWrapper> subjectOrder = 
//            Comparator.comparing(bySubject)
//                    .thenComparing(byProp)
//                    .thenComparing(byObj);
//    private Comparator<TripleWrapper> propertyOrder = 
//            Comparator.comparing(byProp)
//                    .thenComparing(byObj)
//                    .thenComparing(bySubject);
//    private Comparator<TripleWrapper> objectOrder = 
//            Comparator.comparing(byObj)
//                    .thenComparing(bySubject)
//                    .thenComparing(byProp);
    
    public WriterTask(String baseFilename, List<TripleWrapper> listS,List<TripleWrapper> listP,List<TripleWrapper> listO) {
        this.baseFilename = baseFilename;
        this.listS = listS;
        this.listP = listP;
        this.listO = listO;
    }
    
    public void open(String type) throws IOException {
        out = new BufferedOutputStream(new FileOutputStream(baseFilename+type));
    }
    
    public void close() throws IOException {
        out.close();
    }
    
    @Override
    public String call() throws IOException {
        intances++;
        
        Comparator<TripleWrapper> comp=new Comparator<TripleWrapper>(){
            @Override
            public int compare(TripleWrapper o1, TripleWrapper o2) { 
                return o1.getInxData().compareTo(o2.getInxData());
            }
        };        
        
        
        System.out.println("Init Task");
        long time = System.currentTimeMillis();
        open("-sub");
        listS.stream().sorted(comp).forEachOrdered(this::writeToFile);
        close();
        open("-pro");
        listP.stream().sorted(comp).forEachOrdered(this::writeToFile);
        close();
        open("-obj");
        listO.stream().sorted(comp).forEachOrdered(this::writeToFile);
        close();
        System.out.println("time to write "+ baseFilename + "family : "+(System.currentTimeMillis()-time));
        System.out.println("end Task");
        intances--;
        return "Wrote "+baseFilename+" family";
    }
    
    private void writeToFile(TripleWrapper triple) {
        try {
            out.write(triple.getData());
        } catch (IOException ioe) {
            throw new WriterTaskException(ioe);
        }
    }
}

class WriterTaskException extends RuntimeException {
    public WriterTaskException(Exception e) {
        super("Can't convert to utf-8 bytes or can't write to file", e);
    }
}