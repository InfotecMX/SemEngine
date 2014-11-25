package org.semanticwb.store.flatstore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.Comparator;
import java.util.function.Function;
import org.semanticwb.store.TripleWrapper;

/**
 *
 * @author serch
 */
public class WriterTask implements Callable<String>{
    private final String baseFilename;
    final private List<TripleWrapper> list;
    private OutputStream out;
    
    
    private final Function<TripleWrapper, String> bySubject = TripleWrapper::getSubject;
    private final Function<TripleWrapper, String> byProp = TripleWrapper::getProperty;
    private final Function<TripleWrapper, String> byObj = TripleWrapper::getObject;
    
    private Comparator<TripleWrapper> subjectOrder = 
            Comparator.comparing(bySubject)
                    .thenComparing(byProp)
                    .thenComparing(byObj);
    private Comparator<TripleWrapper> propertyOrder = 
            Comparator.comparing(byProp)
                    .thenComparing(byObj)
                    .thenComparing(bySubject);
    private Comparator<TripleWrapper> objectOrder = 
            Comparator.comparing(byObj)
                    .thenComparing(bySubject)
                    .thenComparing(byProp);
    
    public WriterTask(String baseFilename, List<TripleWrapper> list) {
        this.baseFilename = baseFilename;
        this.list = list;
    }
    
    public void open(String type) throws IOException {
        out = new BufferedOutputStream(new FileOutputStream(baseFilename+type));
    }
    
    public void close() throws IOException {
        out.close();
    }
    
    @Override
    public String call() throws IOException {
        long time = System.currentTimeMillis();
        open("-sub");
        list.stream()
            .sorted(subjectOrder)
            .forEachOrdered(this::writeToFile);
        close();
        open("-pro");
        list.stream()
            .sorted(propertyOrder)
            .forEachOrdered(this::writeToFile);
        close();
        open("-obj");
        list.stream()
            .sorted(objectOrder)
            .forEachOrdered(this::writeToFile);
        close();
        System.out.println("time to write "+ baseFilename + "family : "+(System.currentTimeMillis()-time));
        return "Wrote "+baseFilename+" family";
    }
    
    private void writeToFile(TripleWrapper triple) {
        try {
        byte[] sub = triple.getSubject().getBytes("utf-8");
        byte[] prop = triple.getProperty().getBytes("utf-8");
        byte[] obj = triple.getObject().getBytes("utf-8");
        int lt = sub.length + prop.length + obj.length + 20;
        byte[] data = ByteBuffer.allocate(lt).putInt(lt).putInt(sub.length)
                .putInt(prop.length).putInt(obj.length).put(sub).put(prop)
                .put(obj).putInt(lt).array();
        out.write(data);
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