package org.semanticwb.store.flatstore;

import java.nio.ByteBuffer;

/**
 *
 * @author serch
 */
public class FakeTriple {

    private final byte[] bloq;
    private String sub = null;
    private String prop = null;
    private String obj = null;
    private final int iSub;
    private final int iProp;
    private final int iObj;

    public FakeTriple(byte[] bloq) {
        this.bloq = bloq;
        ByteBuffer bb = ByteBuffer.wrap(bloq);
        iSub = bb.getInt(4);
        iProp = bb.getInt(8);
        iObj = bb.getInt(12);
    }

    public byte[] getDataBlock() {
        return bloq;
    }

    public String getSubject() {
        if (null == sub) {
            sub = new String(bloq, 16, iSub);
        }
        return sub;
    }

    public String getProperty() {
        if (null == prop) {
            prop = new String(bloq, 16 + iSub, iProp);
        }
        return prop;
    }

    public String getObject() {
        if (null == obj) {
            obj = new String(bloq, 16 + iSub + iProp, iObj);
        }
        return obj;
    }

}
