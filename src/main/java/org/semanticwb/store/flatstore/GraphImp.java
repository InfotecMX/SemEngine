package org.semanticwb.store.flatstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    private final TripleFileReader subFileReader;
    private final TripleFileReader propFileReader;
    private final TripleFileReader objFileReader;

    public GraphImp(String name, Map<String, String> params) {
        super(name, params);
        String path = getParam("path");
        if (null == path) {
            throw new IllegalArgumentException("Param path is missing in params map");
        }
        File dir = new File(path);
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new IllegalArgumentException("path supplied points to an actual file");
            } else {
                try {
                    openBase(dir);
                    subFileReader = new TripleFileReader(dir.getCanonicalPath()+name+"-sub", this);
                    propFileReader = new TripleFileReader(dir.getCanonicalPath()+name+"-pro", this);
                    objFileReader = new TripleFileReader(dir.getCanonicalPath()+name+"-obj", this);
                } catch (IOException ioe){
                    throw new RuntimeException("Opening a base",ioe);
                }
            }
        } else {
            dir.mkdirs();
            subFileReader=null;
            propFileReader=null;
            objFileReader=null;
        }
        directory = dir;
    }

    @Override
    public String addNameSpace(String prefix, String ns) {
        String localPrefix = null;
        if (null == prefix) {
            localPrefix = Utils.encodeLong(prefixMaps.size());
        } else {
            if (!prefixMaps.containsKey(prefix)) {
                localPrefix = prefix;
            }
        }
        addNameSpace2Cache(localPrefix, ns);
        prefixMaps.put(localPrefix, ns);
        return localPrefix;
    }

    public void createFromNT(String ntFileName) throws IOException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Iterator<Triple> it = read2(ntFileName, 0, 0);
        int count = 0;
        long triples = 0;
        int works = 0;
        List<TripleWrapper> lista = new ArrayList<>((int) (BLOCK_SIZE * 1.2));
        long lecturaStart = System.currentTimeMillis();
        while (it.hasNext()) {
            lista.add(new TripleWrapper(it.next(), this));
            triples++;
            works++;
            if (BLOCK_SIZE == lista.size()) {
                pool.submit(new WriterTask(getFilename(directory, this.getName(), ++count).getCanonicalPath(), lista));
//                if (works % 6 == 0) {
                    Thread.sleep(1000);
//                }
                lista = new ArrayList<>((int) (BLOCK_SIZE * 1.2));

            }
        }
        if (lista.size() > 0) {
            pool.submit(new WriterTask(getFilename(directory, this.getName(), ++count).getCanonicalPath(), lista));
        }
        System.out.println("Lectura y envío de trabajos: " + (System.currentTimeMillis() - lecturaStart));
        System.out.println("triples: " + triples);
        Thread.sleep(1000);
        pool.shutdown();
        while (!pool.awaitTermination(1, TimeUnit.SECONDS)) System.out.println("Waiting...");; //Necesitamos esperar a que los archivos parciales se hayan escrito
        System.out.println("Escritura paso 1: " + (System.currentTimeMillis() - lecturaStart));

        compact(count, triples);

        savePrefixes();

    }

    public static File getFilename(File directory, String graphName, int part) {
        String sPart = "00000" + part;
        return new File(directory, graphName + "_" + sPart.substring(sPart.length() - 5));
    }

    private void compact(int numberChunks, long triples) throws FileNotFoundException {
        Thread compactor = new Thread(new ConsolidatorTask(numberChunks, getName(), directory, triples, 0));
        compactor.start();
        compactor = new Thread(new ConsolidatorTask(numberChunks, getName(), directory, triples, 1));
        compactor.start();
        compactor = new Thread(new ConsolidatorTask(numberChunks, getName(), directory, triples, 2));
        compactor.start();
    }

    private void savePrefixes() throws IOException {
        long time;
        try (ObjectOutputStream outputFile = new ObjectOutputStream(new FileOutputStream(new File(directory, getName() + ".prfx")))) {
            System.out.println("prefixes:" + prefixMaps.size());
            time = System.currentTimeMillis();
            outputFile.writeInt(prefixMaps.keySet().size());
            prefixMaps.keySet().stream().forEachOrdered(entry -> saveEntry(entry, prefixMaps.get(entry), outputFile));
        }
        System.out.println("prefixes time:" + (System.currentTimeMillis() - time));
    }

    private void saveEntry(String key, String value, ObjectOutputStream oos) {
        try {
            oos.writeUTF(key);
            oos.writeUTF(value);
        } catch (IOException ioe) {
            throw new RuntimeException("Salving prefixes", ioe);
        }
    }
    
    
    private void loadPrefixes(File dir) throws IOException {
        try (ObjectInputStream inputFile = new ObjectInputStream(new FileInputStream(new File(dir, getName() + ".prfx")))) {
            int size = inputFile.readInt();
            for (int i=0; i < size; i++){
                String key = inputFile.readUTF();
                String value = inputFile.readUTF();
                prefixMaps.put(key, value);
            }
        }
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

    private void openBase(File dir) throws IOException {
        loadPrefixes(dir);
        
    }

}
