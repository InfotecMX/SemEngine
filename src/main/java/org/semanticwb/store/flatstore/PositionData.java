/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.semanticwb.store.flatstore;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author javiersolis
 */
public class PositionData 
{
    private TripleFileReader reader;
    private long position;
    private byte data[];
    private int nextLen=0;
    private int backLen=0;
    private int str_off=16;
    private int str_len=0;
    private int data_len=0;

    public PositionData(TripleFileReader reader, long position) throws IOException
    {
        this.reader=reader;
        this.position=position;
        data_len=reader.getDataInt(position);       
        
        if(position<reader.dataLength())
        {
            if(position-4>0 && position+data_len+4<reader.dataLength())
            {
                data=reader.getData(position-4,data_len+8);
                backLen=ByteBuffer.wrap(data,0,4).getInt();
                nextLen=ByteBuffer.wrap(data,data.length-4,4).getInt();
                str_off=16+4;
                str_len=data_len-20;
            }else if(position-4>0)
            {
                data=reader.getData(position-4,data_len+4);
                backLen=ByteBuffer.wrap(data,0,4).getInt();
                nextLen=0;
                str_off=16+4;
                str_len=data_len-20;
            }else
            {
                data=reader.getData(position,data_len+4);
                backLen=0;
                nextLen=ByteBuffer.wrap(data,data.length-4,4).getInt();
                str_off=16;
                str_len=data_len-20;
            }
        }
        else
            throw new ArrayIndexOutOfBoundsException("Fuero de rango...");        
    }
        
    
    public PositionData(TripleFileReader reader, long position, byte[] data, int nextLen, int backLen) 
    {
        this.reader=reader;
        this.position = position;
        this.data = data;
        this.nextLen=nextLen;
        this.backLen=backLen;
        
        data_len=data.length;
        str_len=data.length-20;
        if(backLen>0)
        {
            str_off+=4;
            str_len-=4;
            data_len-=4;
        }
        if(nextLen>0)
        {
            str_len-=4;
            data_len-=4;
        }        
    }
    
    public long getPosition() {
        return position;
    }
/*
    public void setPosition(long position) {
        this.position = position;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
*/
    public int getBackLen() {
        return backLen;
    }

    public int getNextLen() {
        return nextLen;
    }
    
    public String getText()
    {
        return new String(data,str_off,str_len);
    }
    
    public String[] getTripleData()
    {
        String ret[]=new String[3];
        int off=0;
        if(backLen>0)off=4;
        ByteBuffer bb = ByteBuffer.wrap(data);
        int i1 = bb.getInt(4+off);
        int i2 = bb.getInt(8+off);
        int i3 = bb.getInt(12+off);
        ret[0]=new String(data, 16+off, i1-1);
        ret[1]=new String(data, 16+off+i1, i2-1);
        ret[2]=new String(data, 16+off+i1+i2, i3-1);   
        return ret;
    }
    
    public String getText(int len)
    {
        if(len<str_len)return new String(data,str_off,len);
        return new String(data,str_off,str_len);
    }    
    
    public PositionData next() throws IOException
    {
        long nposition=position+data_len;
        if(nposition<reader.dataLength())
        {
            if(nposition-4>=0 && nposition+nextLen+4<=reader.dataLength())
            {
                byte arr[]=reader.getData(nposition-4,nextLen+8);
                int b=ByteBuffer.wrap(arr,0,4).getInt();
                int n=ByteBuffer.wrap(arr,arr.length-4,4).getInt();
                return new PositionData(reader, nposition, arr,n,b);
            }else if(nposition-4>=0)
            {
                byte arr[]=reader.getData(nposition-4,nextLen+4);
                int b=ByteBuffer.wrap(arr,0,4).getInt();
                return new PositionData(reader, nposition, arr,0,b);                
            }else
            {
                byte arr[]=reader.getData(nposition,nextLen+4);
                int n=ByteBuffer.wrap(arr,arr.length-4,4).getInt();
                return new PositionData(reader, nposition, arr,n,0);                
            }
        }
        else
            return null;
    }
    
    public PositionData back() throws IOException
    {
        long nposition=position-backLen;
        if(backLen>0)
        {
            if(nposition-4>=0 && nposition+backLen+4<=reader.dataLength())
            {
                byte arr[]=reader.getData(nposition-4,backLen+8);
                int b=ByteBuffer.wrap(arr,0,4).getInt();
                int n=ByteBuffer.wrap(arr,arr.length-4,4).getInt();
                return new PositionData(reader, nposition, arr,n,b);
            }else if(nposition-4>=0)
            {
                byte arr[]=reader.getData(nposition-4,backLen+4);
                int b=ByteBuffer.wrap(arr,0,4).getInt();
                return new PositionData(reader, nposition, arr,0,b);                
            }else
            {
                byte arr[]=reader.getData(nposition,backLen+4);
                int n=ByteBuffer.wrap(arr,arr.length-4,4).getInt();
                return new PositionData(reader, nposition, arr,n,0);                
            }
        }
        else
            return null;
    }    
    
}
