package org.semanticwb.store.flatstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.semanticwb.store.Graph;
import org.semanticwb.store.SObject;
import org.semanticwb.store.SObjectIterator;
import org.semanticwb.store.Triple;
import org.semanticwb.store.TripleWrapper;
import org.semanticwb.store.utils.Utils;

/**
 *
 * @author serch
 */
public class GraphImp extends Graph {
    
    private final int BLOCK_SIZE = 500_000;
    private ConcurrentHashMap<String, String> prefixMaps = new ConcurrentHashMap<>();
    private final File directory;

    public GraphImp(String name, Map<String, String> params) {
        super(name, params);
        String path=getParam("path");
        if (null==path) throw new IllegalArgumentException("Param path is missing in params map");
        File dir = new File(path);
        if (dir.exists()) {
            if (!dir.isDirectory()) throw new IllegalArgumentException("path supplied points to an actual file");
            else openBase(dir);
        } else
            dir.mkdirs();
        directory = dir;
    }

    @Override
    public String addNameSpace(String prefix, String ns) {
        String localPrefix = null;
        if (null == prefix) {
            localPrefix=Utils.encodeLong(prefixMaps.size());
        } else {
            if (!prefixMaps.containsKey(prefix))
            localPrefix = prefix;
        }
        addNameSpace2Cache(localPrefix,ns);
        prefixMaps.put(localPrefix, ns);
        return localPrefix;
    }
    
    public void createFromNT(String ntFileName) throws IOException, InterruptedException{
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Iterator<Triple> it = read2(ntFileName, 0, 0);
        int count = 0;
        long triples = 0;
        List<TripleWrapper> lista = new ArrayList<>((int)(BLOCK_SIZE * 1.2));
        long lecturaStart = System.currentTimeMillis();
        while(it.hasNext()){
            lista.add(new TripleWrapper(it.next(),this)); triples++;
            if (BLOCK_SIZE == lista.size()){
                pool.submit(new WriterTask(getFilename(directory, this.getName(), ++count), lista));
                lista = new ArrayList<>((int)(BLOCK_SIZE * 1.2));
            }
        }
        if (lista.size()>0){
            pool.submit(new WriterTask(getFilename(directory, this.getName(), ++count), lista));
        }
        System.out.println("Lectura y envío de trabajos: "+ (System.currentTimeMillis() - lecturaStart));
        System.out.println("triples: "+ triples);
        pool.shutdown();
        while(!pool.awaitTermination(1, TimeUnit.SECONDS)); //Necesitamos esperar a que los archivos parciales se hayan escrito
        System.out.println("Escritura paso 1: "+ (System.currentTimeMillis() - lecturaStart));
        
        compact(count, triples);
        
        
    }
    
    public static File getFilename(File directory, String graphName, int part){
        String sPart = "00000"+part;
        return new File(directory, graphName+"_"+sPart.substring(sPart.length()-5));
    }
    
    private void compact(int numberChunks, long triples) throws FileNotFoundException{ 
        Runnable compactor = new ConsolidatorTask(numberChunks, getName(), directory, triples);
        compactor.run();
    }
    
    @Override
    public boolean addTriple(Triple triple, boolean thread) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean addTriple(Triple triple) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    

    @Override
    public void begin() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void synchDB() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected SObjectIterator findSObjects(SObject obj, boolean reverse) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected boolean removeSObject(SObject obj) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected boolean addSObject(SObject obj, boolean thread) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeNameSpace(String prefix) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long count() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void openBase(File dir) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
