package org.semanticwb.store.flatstore;

import org.semanticwb.store.TripleWrapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.semanticwb.store.Graph;
import org.semanticwb.store.SObject;
import org.semanticwb.store.SObjectIterator;
import org.semanticwb.store.Triple;
import org.semanticwb.store.utils.Utils;

/**
 *
 * @author serch
 */
public class GraphImp extends Graph {

    private final int BLOCK_SIZE = 500_000;
    private ConcurrentHashMap<String, String> prefixMaps = new ConcurrentHashMap<>();
    private final File directory;
    public final TripleFileReader subFileReader;
    public final TripleFileReader propFileReader;
    public final TripleFileReader objFileReader;
    private boolean isClosed = true;
    
    private Long t_size=null;

    public GraphImp(String name, Map<String, String> params) {
        super(name, params);
        String path = getParam("path");
        if (null == path) {
            throw new IllegalArgumentException("Param path is missing in params map");
        }
        File dir = new File(path, name);
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new IllegalArgumentException("path and name supplied points to an actual file");
            } else {
                try {
                    openBase(dir);
                    subFileReader = new TripleFileReader(dir.getCanonicalPath() + "/" + name + "-sub", this, IdxBy.SUBJECT);
//                    System.out.println("Groups-S: " + subFileReader.size());
//                    System.out.println("Triples-S: " + subFileReader.countTriples());
                    propFileReader = new TripleFileReader(dir.getCanonicalPath() + "/" + name + "-pro", this, IdxBy.PROPERTY);
//                    System.out.println("Groups-P: " + propFileReader.size());
//                    System.out.println("Triples-P: " + propFileReader.countTriples());
                    objFileReader = new TripleFileReader(dir.getCanonicalPath() + "/" + name + "-obj", this, IdxBy.OBJECT);
//                    System.out.println("Groups-O: " + objFileReader.size());
//                    System.out.println("Triples-O: " + objFileReader.countTriples());
                    isClosed = false;
                } catch (IOException ioe) {
                    throw new RuntimeException("Opening a base", ioe);
                }
            }
        } else {
            dir.mkdirs();
            subFileReader = null;
            propFileReader = null;
            objFileReader = null;
        }
        directory = dir;
        setEncodeMaxURIs(false);
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

    public void createFromNT(String... ntFileName) throws IOException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        
        int count = 0;
        long triples = 0;
//
//        Comparator comp=(Comparator<TripleWrapper>) (TripleWrapper o1, TripleWrapper o2) -> {
//            if(o1.getInxData().compareTo(o2.getInxData())>0)return 1;
//            else return -1;
//        };
        
//        TreeSet<TripleWrapper> listaS = new TreeSet<TripleWrapper>(comp);
//        TreeSet<TripleWrapper> listaP = new TreeSet<TripleWrapper>(comp);
//        TreeSet<TripleWrapper> listaO = new TreeSet<TripleWrapper>(comp);
        long lecturaStart=0, time2sub=0;
        for(String currentNtFile: ntFileName){
        Iterator<Triple> it = read2(currentNtFile, 0, 0);
        ArrayList<TripleWrapper> listaS = new ArrayList<>(BLOCK_SIZE);
        ArrayList<TripleWrapper> listaP = new ArrayList<>(BLOCK_SIZE);
        ArrayList<TripleWrapper> listaO = new ArrayList<>(BLOCK_SIZE);

        lecturaStart = System.currentTimeMillis();
        time2sub = lecturaStart;
        while (it.hasNext()) {
            listaS.add(new TripleWrapper(it.next(), this, IdxBy.SUBJECT));
            listaP.add(new TripleWrapper(it.next(), this, IdxBy.PROPERTY));
            listaO.add(new TripleWrapper(it.next(), this, IdxBy.OBJECT));
            triples++;
            if (BLOCK_SIZE == listaS.size()) {
                System.out.println("DataGathered "+(count+1)+":"+(System.currentTimeMillis()-time2sub));
                System.out.println("prefixes: "+prefixMaps.size());
                System.out.println("f-Memory: "+Runtime.getRuntime().freeMemory());
                while (WriterTask.intances > 1) {
                    System.out.println("Waiting to submit job...");
                    Thread.sleep(500);
                }
                //System.out.println("Add Task");
                pool.submit(new WriterTask(getFilename(directory, this.getName(), ++count).getCanonicalPath(), listaS,listaP,listaO));
                listaS = new ArrayList<>(BLOCK_SIZE);
                listaP = new ArrayList<>(BLOCK_SIZE);
                listaO = new ArrayList<>(BLOCK_SIZE);
                time2sub=System.currentTimeMillis();
            }
        }
        if (listaS.size() > 0) {
                System.out.println("DataGathered "+(count+1)+":"+(System.currentTimeMillis()-time2sub));
                pool.submit(new WriterTask(getFilename(directory, this.getName(), ++count).getCanonicalPath(), listaS,listaP,listaO));
        }
        }
        System.out.println("Lectura y envío de trabajos: " + (System.currentTimeMillis() - lecturaStart));
        System.out.println("triples: " + triples);
        Thread.sleep(1000);
        pool.shutdown();
        while (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
            System.out.println("Waiting...");
        }; //Necesitamos esperar a que los archivos parciales se hayan escrito
        System.out.println("Escritura paso 1: " + (System.currentTimeMillis() - lecturaStart));

        compact(count, triples);

        savePrefixes();

    }

    public static File getFilename(File directory, String graphName, int part) {
        String sPart = "00000" + part;
        return new File(directory, graphName + "_" + sPart.substring(sPart.length() - 5));
    }

    private void compact(int numberChunks, long triples) throws FileNotFoundException {
        Thread compactor = new Thread(new ConsolidatorTask(numberChunks, getName(), directory, triples, IdxBy.SUBJECT));
        compactor.start();
        compactor = new Thread(new ConsolidatorTask(numberChunks, getName(), directory, triples, IdxBy.PROPERTY));
        compactor.start();
        compactor = new Thread(new ConsolidatorTask(numberChunks, getName(), directory, triples, IdxBy.OBJECT));
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
        File file = new File(dir, getName() + ".prfx");
        System.out.println("file:" + file.getCanonicalPath());
        try (ObjectInputStream inputFile = new ObjectInputStream(new FileInputStream(file))) {
            int size = inputFile.readInt();
            for (int i = 0; i < size; i++) {
                String key = inputFile.readUTF();
                String value = inputFile.readUTF();
                addNameSpace(key, value);
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
        return isClosed;
    }

    @Override
    public void close() {
        try {
            isClosed = true;
            if (null != subFileReader) {
                subFileReader.close();
            }
            if (null != propFileReader) {
                propFileReader.close();
            }
            if (null != objFileReader) {
                objFileReader.close();
            }
        } catch (IOException ioe) {
            //TODO: Log Error closing files
        }
    }

    @Override
    public void synchDB() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected SObjectIterator findSObjects(SObject obj, boolean reverse) 
    {
        TripleFileReader ind=subFileReader;
        int idx=0;
        boolean full=false;
        //System.out.println(new String(propFileReader.getDataBlock(propFileReader.findGroup(obj.p).getPosition())));
        final String s = obj.s;
        final String p = obj.p;
        final String o = obj.o;
        
        String group=s+TripleFileReader.SEPARATOR;
        String txt="";
        
        if(s!=null)
        {
            ind=subFileReader;
            group=s+TripleFileReader.SEPARATOR;
            idx=0;
            txt=s+TripleFileReader.SEPARATOR;
            if(p!=null)
            {
                txt=txt+p+TripleFileReader.SEPARATOR;
                group=txt;
                if(o!=null)
                {
                    txt=txt+o+TripleFileReader.SEPARATOR;
                    full=true;
                }
            }else if(o!=null)
            {
                ind=objFileReader;
                //group=o;
                idx=2;
                txt=o+TripleFileReader.SEPARATOR+s+TripleFileReader.SEPARATOR;
                group=txt;
            }
        }else if(p!=null)
        {
            ind=propFileReader;
            group=p+TripleFileReader.SEPARATOR;
            idx=1;
            txt=p+TripleFileReader.SEPARATOR;
            if(o!=null)
            {
                txt=txt+o+TripleFileReader.SEPARATOR;
                group=txt;
            }
        }else if(o!=null)
        {
            ind=objFileReader;
            group=o+TripleFileReader.SEPARATOR;
            idx=2;
            txt=o+TripleFileReader.SEPARATOR;
        }
        
        SObjectIterator ret=null;
        
        if(full)
        {
            final String val;
            try
            {
                final PositionData pd=ind.findData(group, txt);
                if(pd.getText().startsWith(txt))val="found";
                else val=null;
            }catch(Exception e)
            {
                throw new RuntimeException(e);
            }
            
            ret = new SObjectIterator()
            {                
                boolean next=false;
                
                @Override
                public boolean hasNext()
                {
                    return !next && val!=null;
                }

                @Override
                public SObject next()
                {
                    next=true;
                    return obj;
                }

                @Override
                public void remove()
                {
                    removeSObject(obj);
                }
                
                @Override
                public void close()
                {                    
                }

                @Override
                public boolean isClosed()
                {
                    return next;
                }

                @Override
                public long count()
                {
                    return hasNext()?1:0;
                }
            };
        }else
        {            
            final PositionData pd;
            final String key=txt;
            final int idxt=idx;
            try
            {
                pd=ind.findData(group, txt);
            }catch(Exception e)
            {
                throw new RuntimeException(e);
            }

//            if(!reverse)
//            {            
                ret = new SObjectIterator()
                {            
                    PositionData act=pd;
                    long c=1;
                    boolean closed=false;

                    @Override
                    public boolean hasNext()
                    {  
                        boolean ret=act!=null && act.getText().startsWith(key);
                        if(!ret && !closed)
                        {
                            closed=true;
                            c--;
                        }
                        return ret;
                    }

                    @Override
                    public SObject next()
                    {
                        SObject ret=null;
                               
                        String t[]=act.getTripleData();
                        if(idxt==0)ret=new SObject(t[0],t[1],t[2],"");
                        if(idxt==1)ret=new SObject(t[2],t[0],t[1],"");
                        if(idxt==2)ret=new SObject(t[1],t[2],t[0],"");
                        
                        try
                        {
                            act=act.next();
                            c++;
                        }catch(IOException e)
                        {
                            e.printStackTrace();
                            act=null;
                        }
                        
                        return ret;
                    }

                    @Override
                    public void remove()
                    {                   
                    }

                    @Override
                    public void close()
                    {
                        if(!closed)
                        {
                            closed=true;
                        }
                    }

                    @Override
                    public boolean isClosed()
                    {
                        return closed;
                    }

                    @Override
                    public long count()
                    {           
                        //TODO:mejorar
                        while(hasNext())
                        {
                            next();
                        }
                        return c;
                    }                
                };
//            }else
//            {
//                if(key.length>0)cur.seek(factory.bytes(txt+(char)1));
//                ret = new SObjectIterator()
//                {                                
//                    long c=0;
//                    Map.Entry<byte[], byte[]> tmp=null;
//
//                    Map.Entry<byte[], byte[]> act;
//                    boolean closed=false;
//
//                    @Override
//                    public boolean hasNext()
//                    {          
//                        if(tmp==null && cur.hasPrev())tmp=cur.prev();                    
//                        boolean ret=tmp!=null && Utils.startWidth(tmp.getKey(), key);
//                        if(!ret)this.close();
//                        return ret;
//                    }
//
//                    @Override
//                    public SObject next()
//                    {
//                        SObject ret=null;
//                        if(!closed)
//                        {                    
//                            c++;
//                            ret=new SObject(factory.asString(tmp.getKey()), factory.asString(tmp.getValue()), fidx);
//                            act=tmp;
//                            if(cur.hasPrev())tmp=cur.prev();
//                            else tmp=null;
//                        }
//                        return ret;
//                    }
//
//                    @Override
//                    public void remove()
//                    {
//                        String t[]=factory.asString(tmp.getKey()).split(""+separator);
//
//                        String ts=null,tp=null,to=null;
//
//                        if(fidx==0)
//                        {
//                            ts=t[0];
//                            tp=t[1];
//                            to=t[2];
//                        }else if(fidx==1)
//                        {
//                            ts=t[2];
//                            tp=t[0];
//                            to=t[1];
//                        }else if(fidx==2)
//                        {
//                            ts=t[1];
//                            tp=t[2];
//                            to=t[0];
//                        }                    
//                        removeSObject(new SObject(ts,tp,to,null));                    
//                    }
//
//                    @Override
//                    public void close()
//                    {
//                        if(!closed)
//                        {
//                            try
//                            {
//                                cur.close();;
//                            }catch(Throwable e)
//                            {
//                                e.printStackTrace();
//                            }
//                            closed=true;
//                        }
//                    }
//
//                    @Override
//                    public boolean isClosed()
//                    {
//                        return closed;
//                    }
//
//                    @Override
//                    public long count()
//                    {               
//                        while(cur.hasPrev())
//                        {
//                            Map.Entry<byte[], byte[]> tmp=cur.prev();
//                            if(!Utils.startWidth(tmp.getKey(), key))break;
//                            c++;
//                        }
//                        this.close();
//                        return c;
//                    }                
//                };                
//            }
                
            //TODO:Mejorar    
            if(reverse)
            {
                final ArrayList<SObject> arr=new ArrayList();
                while (ret.hasNext())
                {
                    SObject sObject = ret.next();
                    arr.add(sObject);
                }
                
                final ListIterator<SObject> it=arr.listIterator(arr.size());
                
                return new SObjectIterator()
                {
                    SObject tmp=null;

                    @Override
                    public boolean hasNext()
                    {
                        return it.hasPrevious();
                    }

                    @Override
                    public SObject next()
                    {
                        tmp=it.previous();
                        return tmp;
                    }

                    @Override
                    public long count()
                    {
                        return (long)arr.size();
                    }

                    @Override
                    public void remove()
                    {
                        removeSObject(tmp);
                    }
                };
            }        
        }
        return ret;

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
    public long count()
    {
        if(t_size==null)
        {
            try
            {
                t_size = propFileReader.countTriples();
            }catch(IOException e)
            {
                //throw new RuntimeException(e);
                e.printStackTrace();
                return -1;
            }
        }
        return t_size;
    }

    private void openBase(File dir) throws IOException {
        loadPrefixes(dir);
    }

}
