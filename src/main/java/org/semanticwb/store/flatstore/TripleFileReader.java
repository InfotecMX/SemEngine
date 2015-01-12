package org.semanticwb.store.flatstore;

import org.semanticwb.store.TripleWrapper;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
    private final int FIND_SEC=10;

    public TripleFileReader(String baseFilename, Graph graphReference, IdxBy ordering) throws IOException {
        this.baseFilename = baseFilename;
        this.graphReference = graphReference;
        this.ordering = ordering;
        data = new RandomAccessFile(this.baseFilename + ".swbdb", "r");
        idx = new RandomAccessFile(this.baseFilename + ".idx", "r");
        this.length =idx.length();
        this.dataLength=data.length();
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
    
    public Triple[] getTripleGroup(long index) throws IOException 
    {
        IdxData idx=getIdxData(index);
        Triple[] ret = new Triple[idx.getNumObjects()];
        long currPosition = idx.getPosition();
        for (int i = 0; i < idx.getNumObjects(); i++) {
            byte[] buff = getDataBlock(currPosition);
            ret[i] = getTripleFromData(buff);
            currPosition += buff.length;
        }
        return ret;
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
        String groupIni=group+(char)0;
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
    
    public PositionData findData(String group, String match) throws IOException
    {
        IdxData idx=findGroup(group);
        if(idx==null)return null;
        
        long p=idx.getPosition();
        PositionData datap=new PositionData(this, p);
        
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
                        
//                        System.out.println("g:"+getGroup(i));
//                        long currPosition=data.getPosition();
//                        byte[] buff = getDataBlock(currPosition);
//                        Triple t = getTripleFromData(buff);
//                        System.out.println("Triple:"+t);
                    }
                    c_triples=ret;
                }
            }
        }
        return c_triples;
    }    
    
    
    private Triple getTripleFromData(byte[] buff){
        Triple ret =null;
        switch (ordering) {
            case SUBJECT: 
                ret = TripleWrapper.getTripleFromDataBySubject(graphReference, buff);
                break;
            case PROPERTY: 
                ret = TripleWrapper.getTripleFromDataByProperty(graphReference, buff);
                break;
            case OBJECT: 
                ret = TripleWrapper.getTripleFromDataByObject(graphReference, buff);
                break;
        }
        return ret;
    } 

    public long getFileDataLength() {
        return dataLength;
    }
    
    
    
    

    
/*    
    private Triple getTripleAt(long position) throws IOException {
        return getTripleFromData(getDataBlock(position));
    }
    
    public Triple getNextTriple() throws IOException 
    {
        return getTripleFromData(getDataBlock(dataPosition));
    }
    
    public Triple getPreviousTriple() throws IOException 
    {
        data.seek(dataPosition-4);
        byte[] header = new byte[4];
        data.read(header);
        int size = ByteBuffer.wrap(header).getInt();
        return getTripleFromData(getDataBlock(dataPosition));
    }    
    
    public Triple findTriple(String str)
    {
        
    }

    private IdxData getIdxData(long idxPosition) throws IOException {
        idx.seek(idxPosition);
        byte[] idxData = new byte[12];
        idx.read(idxData);
        ByteBuffer bb = ByteBuffer.wrap(idxData);
        return new IdxData(bb.getLong(), bb.getInt());
    }

    private IdxData getNextIdxData() throws IOException {
        idxPosition += 12;
        return getIdxData();
    }
    


    private Triple[] getTripleGroup(IdxData idx) throws IOException {
        Triple[] ret = new Triple[idx.getNumObjects()];
        long currPosition = idx.getPosition();
        for (int i = 0; i < idx.getNumObjects(); i++) {
            byte[] buff = getDataBlock(currPosition);
            ret[i] = getTripleFromData(buff);
            currPosition += buff.length;
        }
        return ret;
    }

    private byte[] getDataBlock(long position) throws IOException {
        data.seek(position);
        byte[] header = new byte[4];
        data.read(header);
        int size = ByteBuffer.wrap(header).getInt();
        byte[] buff = new byte[size];
        data.seek(position);
        data.read(buff);
        return buff;
    }

    private String getGroupValue(long position) throws IOException {
        byte[] buff = getDataBlock(position);
        int subSize = ByteBuffer.wrap(buff).getInt(4);
        return new String(buff, 16, subSize, "utf-8");
    }
*/

    public long getFileIdxLength() {
        return length;
    }
    
    public long getIdxLength() {
        return length/12;
    }

}
