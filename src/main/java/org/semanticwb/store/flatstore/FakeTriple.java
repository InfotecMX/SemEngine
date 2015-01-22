package org.semanticwb.store.flatstore;

import java.nio.ByteBuffer;

/**
 *
 * @author serch
 */
public class FakeTriple {

    private final byte[] bloq;
    private final String data;
    private final String group;

    public FakeTriple(byte[] bloq) {
        this.bloq = bloq;
        ByteBuffer bb = ByteBuffer.wrap(bloq);
        
        data = new String(bloq, 16, bloq.length-20);
        //group = new String(bloq, 16, bb.getInt(4));        
        group = new String(bloq, 16, bb.getInt(4)+bb.getInt(8));
        
//        sizes[0] = bb.getInt(4);
//        sizes[1] = bb.getInt(8);
//        sizes[2] = bb.getInt(12);
    }

    public byte[] getDataBlock() {
        return bloq;
    }
    
    public String getData() {
        return data;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj!=null && obj instanceof FakeTriple)
        {
            return this.data.equals(((FakeTriple)obj).data);
        }
        return false;
    }
    
}
