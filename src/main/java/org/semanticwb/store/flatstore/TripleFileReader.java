package org.semanticwb.store.flatstore;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.semanticwb.store.TripleWrapper;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.semanticwb.store.Graph;
import org.semanticwb.store.Triple;

/**
 *
 * @author serch
 */
public class TripleFileReader {

    private final RandomAccessFile data;
    private final RandomAccessFile idx;
    private final String baseFilename;
    private final Graph graphReference;
    private final IdxBy ordering;
    private final long length;
    private final long dataLength;
    private Long c_triples=null;
    private final int FIND_SEC=50;
    
    public static final char SEPARATOR=0;
    
    private final int CACHE=0;
    private LoadingCache<String, PositionData> cache = null;    

    public TripleFileReader(String baseFilename, Graph graphReference, IdxBy ordering) throws IOException {
        this.baseFilename = baseFilename;
        this.graphReference = graphReference;
        this.ordering = ordering;
        data = new RandomAccessFile(this.baseFilename + ".swbdb", "r");
        idx = new RandomAccessFile(this.baseFilename + ".idx", "r");
        this.length =idx.length();
        this.dataLength=data.length();
        
        final TripleFileReader _this=this;
        
        if(CACHE>0)
        {
            cache = CacheBuilder.newBuilder()
                    .maximumSize(CACHE)
                    .expireAfterAccess(10, TimeUnit.MINUTES)
                    .build(new CacheLoader<String, PositionData>()
                    {
                        @Override
                        public PositionData load(String key) throws IOException
                        {
                            IdxData idx=findGroup(key);
                            if(idx==null)return null;

                            long p=idx.getPosition();
                            return new PositionData(_this, p);
                        }
                    });
        }
        
        
    }
    
    public synchronized IdxData getIdxData(long index) throws IOException {
        idx.seek(index*12);
        byte[] idxData = new byte[12];
        idx.read(idxData);
        ByteBuffer bb = ByteBuffer.wrap(idxData);
        return new IdxData(bb.getLong(), bb.getInt());
    }
    
    protected synchronized byte[] getData(long position, int lenght) throws IOException {
        data.seek(position);
        byte[] val = new byte[lenght];
        data.read(val);
        return val;
    }
    
    protected int getDataInt(long position) throws IOException 
    {
        byte[] header = getData(position,4);
        return ByteBuffer.wrap(header).getInt();
    }
    
    protected byte[] getDataBlock(long position) throws IOException {
        return getData(position,getDataInt(position));
    }  

    public String getGroup(long index) throws IOException
    {
        return getGroup(index, 0);
    }
    
    public String getGroup(long index, int len) throws IOException
    {
        IdxData data=getIdxData(index);
        long currPosition=data.getPosition();
        int s=0;
        if(len>0)s=len;
        //else s=getDataInt(currPosition+4);
        else s=getDataInt(currPosition+4)+getDataInt(currPosition+8);
        if(len>0 && len<s)s=len;
        byte[] buff = getData(currPosition+16,s);  //getDataBlock(currPosition);
        return new String(buff);
    }
    
    public IdxData findInterGroup(String group, long index, long jump) throws IOException
    {
//        System.out.println("findInterGroup:"+group+":"+getGroup(index,group.length())+","+index+","+jump);
        int c=group.compareTo(getGroup(index,group.length()));
        if(c==0)return getIdxData(index);
        else if(c>0)
        {
//            System.out.println(">");
            if(jump<FIND_SEC)
            {
//                System.out.println("FIND_SEC:"+(index+1)+"<"+(index+jump));
                for(long i=index;i<=index+jump;i++)
                {
                    if(group.compareTo(getGroup(i,group.length()))<=0)return getIdxData(i);       
                }
                throw new RuntimeException("Error de logica...");
            }else
            {
                return findInterGroup(group, index+jump/2,jump%2==1?jump/2+1:jump/2);
            }
        }else
        {
//            System.out.println("<");
            if(jump<FIND_SEC)
            {
//                System.out.println("FIND_SEC:"+(index-jump)+"<"+(index));
                for(long i=(index-jump)>0?index-jump:0;i<=index;i++)
                {
//                    System.out.println("findSec:"+group+":"+getGroup(i)+","+i+","+jump);
                    if(group.compareTo(getGroup(i,group.length()))<=0)return getIdxData(i);       
                }
                System.out.println("FIND_SEC:"+(index-jump)+"<"+(index));                
                throw new RuntimeException("Error de logica...");
            }else
            {
                return findInterGroup(group, index-jump/2,jump%2==1?jump/2+1:jump/2);
            }            
        }
    }
    
    public IdxData findGroup(String group) throws IOException
    {
        String groupIni=group+TripleFileReader.SEPARATOR;
        long index=0;
        long jump=size();
        
        int c=groupIni.compareTo(getGroup(index,groupIni.length()));
        if(c<=0)return getIdxData(index);
        else
        {
            index+=jump-1;
            c=groupIni.compareTo(getGroup(index,groupIni.length()));
            if(c==0)return getIdxData(index);
            else if(c>0)return null;
            else
            {
                return findInterGroup(groupIni, index/2,jump/2);
            }
        }
    }
    
    public PositionData findData(String group, String match) throws IOException, ExecutionException
    {
        PositionData datap=null;
        if(CACHE>0)
        {
            if(group==null)return null;
            datap=cache.get(group);
        }else
        {        
            IdxData idx=findGroup(group);
            if(idx==null)return null;

            long p=idx.getPosition();
            datap=new PositionData(this, p);
        }
        
        while(datap!=null)
        {
            String txt=datap.getText(match.length());
            if(txt.compareTo(match)>=0) return datap;
            datap=datap.next();
        }
        return null;
    }
    
    
    
    /**
     * Number of indexes elements
     * @return 
     */
    protected long size() {
        return length / 12;
    }
    
    protected long idxLength()
    {
        return length;
    }
    
    protected long dataLength()
    {
        return dataLength;
    }    
    
    protected void close() throws IOException {
        if (null != data) {
            data.close();
        }
        if (null != idx) {
            idx.close();
        }
    }
    
    
    /**
     * return the number of triples
     * @return
     * @throws IOException 
     */
    protected long countTriples() throws IOException {
        if(c_triples==null)
        {
            synchronized(this)
            {
                if(c_triples==null)
                {
                    long ret = 0;
                    long size = size();
                    for (int i = 0; i < size; i++) {
                        IdxData data=getIdxData(i);
                        ret += data.getNumObjects();
                    }
                    c_triples=ret;
                }
            }
        }
        return c_triples;
    }    

    public long getFileDataLength() {
        return dataLength;
    }
    

    public long getFileIdxLength() {
        return length;
    }
    
    public long getIdxLength() {
        return length/12;
    }

}
