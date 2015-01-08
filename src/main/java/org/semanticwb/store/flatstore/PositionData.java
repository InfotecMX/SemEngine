/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.semanticwb.store.flatstore;

import java.io.IOException;

/**
 *
 * @author javiersolis
 */
public class PositionData 
{
    private TripleFileReader reader;
    private long position;
    private byte data[];

    public PositionData(TripleFileReader reader, long position, byte[] data) {
        this.reader=reader;
        this.position = position;
        this.data = data;
    }
    
    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
    
    public String getText()
    {
        return new String(data,16,data.length-20);
    }
    
    public PositionData next() throws IOException
    {
        long nposition=position+data.length;
        if(nposition<reader.dataLength())
            return new PositionData(reader, nposition, reader.getDataBlock(nposition));
        else
            return null;
    }
    
    public PositionData back() throws IOException
    {
        int s=reader.getDataInt(position-4);
        long nposition=position-s;
        if(nposition>0)
            return new PositionData(reader, nposition, reader.getData(nposition,s));
        else 
            return null;
    }    
    
}
