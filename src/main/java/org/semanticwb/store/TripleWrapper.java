package org.semanticwb.store;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.semanticwb.store.flatstore.IdxBy;
import org.semanticwb.store.flatstore.TripleFileReader;

/**
 *
 * @author serch
 */
public class TripleWrapper {
    private String data=null;
    
    public int n1,n2,n3;
    
    public TripleWrapper(Triple triple, Graph graph, IdxBy ind_type) {
        String s=graph.encNode(triple.getSubject());
        String p=graph.encNode(triple.getProperty());
        String o=graph.encNode(triple.getObject());
        switch (ind_type){
            case SUBJECT:
            {
                data=s+TripleFileReader.SEPARATOR+p+TripleFileReader.SEPARATOR+o+TripleFileReader.SEPARATOR;
                n1=s.length()+1;
                n2=p.length()+1;
                n3=o.length()+1;
                break;
            }
            case PROPERTY:
            {
                data=p+TripleFileReader.SEPARATOR+o+TripleFileReader.SEPARATOR+s+TripleFileReader.SEPARATOR;
                n3=s.length()+1;
                n1=p.length()+1;
                n2=o.length()+1;
                break;
            }
            case OBJECT:
            {
                data=o+TripleFileReader.SEPARATOR+s+TripleFileReader.SEPARATOR+p+TripleFileReader.SEPARATOR;
                n2=s.length()+1;
                n3=p.length()+1;
                n1=o.length()+1;
            }
        }
    }

    public String getInxData() {
        return data;
    }

    public byte[] getData() throws UnsupportedEncodingException
    {
        byte[] t = data.getBytes("utf-8");
        int lt = n1 + n2 + n3 + 20;
        return ByteBuffer.allocate(lt).putInt(lt).putInt(n1)
                .putInt(n2).putInt(n3).put(t).putInt(lt).array();
        
    }
    

    public static Triple getTripleFromDataBySubject(Graph graph, byte[] data){
        ByteBuffer bb = ByteBuffer.wrap(data);
        int iSub = bb.getInt(4);
        int iProp = bb.getInt(8);
        int iObj = bb.getInt(12);
        return new Triple(
                graph.decNode(new String(data, 16, iSub)), 
                graph.decNode(new String(data, 16 + iSub, iProp)), 
                graph.decNode(new String(data, 16 + iSub + iProp, iObj)));
    }

    public static Triple getTripleFromDataByProperty(Graph graph, byte[] data){
        ByteBuffer bb = ByteBuffer.wrap(data);
        int iSub = bb.getInt(4);
        int iProp = bb.getInt(8);
        int iObj = bb.getInt(12);
        return new Triple(
                graph.decNode(new String(data, 16 + iSub + iProp, iObj)),
                graph.decNode(new String(data, 16, iSub)), 
                graph.decNode(new String(data, 16 + iSub, iProp)));
    }

    public static Triple getTripleFromDataByObject(Graph graph, byte[] data){
        ByteBuffer bb = ByteBuffer.wrap(data);
        int iSub = bb.getInt(4);
        int iProp = bb.getInt(8);
        int iObj = bb.getInt(12);
        return new Triple(
                graph.decNode(new String(data, 16 + iSub, iProp)), 
                graph.decNode(new String(data, 16 + iSub + iProp, iObj)),
                graph.decNode(new String(data, 16, iSub)));
    }

}
