/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.semanticwb.store;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 *
 * @author serch
 */
public class TripleWrapper {
    private String data=null;
    
    public int n1,n2,n3;
    
    
    public TripleWrapper(Triple triple, Graph graph, int ind_type) {
        String s=graph.encNode(triple.getSubject());
        String p=graph.encNode(triple.getProperty());
        String o=graph.encNode(triple.getObject());
        if(ind_type==1)
        {
            data=s+p+o;
            n1=s.length();
            n2=p.length();
            n3=o.length();
        }else if(ind_type==2)
        {
            data=p+o+s;
            n2=s.length();
            n3=p.length();
            n1=o.length();            
        }else if(ind_type==3)
        {
            data=o+s+p;
            n3=s.length();
            n1=p.length();
            n2=o.length();            
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
    

    public static Triple getTripleFromData(Graph graph, byte[] data){
        ByteBuffer bb = ByteBuffer.wrap(data);
        int iSub = bb.getInt(4);
        int iProp = bb.getInt(8);
        int iObj = bb.getInt(12);
        return new Triple(
                graph.decNode(new String(data, 16, iSub)), 
                graph.decNode(new String(data, 16 + iSub, iProp)), 
                graph.decNode(new String(data, 16 + iSub + iProp, iObj)));
    }


}
