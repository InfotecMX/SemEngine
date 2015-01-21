import java.io.UnsupportedEncodingException;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class LZ4Example {

  public static void main(String[] args) throws Exception {
    example();
  }

  private static void example() throws UnsupportedEncodingException {
    LZ4Factory factory = LZ4Factory.fastestInstance();

    //byte[] data = "12345345234572".getBytes("UTF-8");
    byte[] data = ("LZ4Factory factory = LZ4Factory.fastestInstance();\n" +
"\n" +
"    byte[] data = \"12345345234572\".getBytes(\"UTF-8\");\n" +
"    final int decompressedLength = data.length;\n" +
"\n" +
"    // compress data\n" +
"    LZ4Compressor compressor = factory.fastCompressor();\n" +
"    int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);\n" +
"    byte[] compressed = new byte[maxCompressedLength];\n" +
"    int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);\n" +
"\n" +
"    // decompress data\n" +
"    // - method 1: when the decompressed length is known\n" +
"    LZ4FastDecompressor decompressor = factory.fastDecompressor();\n" +
"    byte[] restored = new byte[decompressedLength];\n" +
"    int compressedLength2 = decompressor.decompress(compressed, 0, restored, 0, decompressedLength);\n" +
"    // compressedLength == compressedLength2\n" +
"\n" +
"    // - method 2: when the compressed length is known (a little slower)\n" +
"    // the destination buffer needs to be over-sized\n" +
"    LZ4SafeDecompressor decompressor2 = factory.safeDecompressor();\n" +
"    int decompressedLength2 = decompressor2.decompress(compressed, 0, compressedLength, restored, 0);\n" +
"    // decompressedLength == decompressedLength2").getBytes("UTF-8");
    final int decompressedLength = data.length;

    // compress data
    LZ4Compressor compressor = factory.fastCompressor();
    int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
    byte[] compressed = new byte[maxCompressedLength];
    int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);

    // decompress data
    // - method 1: when the decompressed length is known
    LZ4FastDecompressor decompressor = factory.fastDecompressor();
    byte[] restored = new byte[decompressedLength];
    int compressedLength2 = decompressor.decompress(compressed, 0, restored, 0, decompressedLength);
    // compressedLength == compressedLength2

    // - method 2: when the compressed length is known (a little slower)
    // the destination buffer needs to be over-sized
    LZ4SafeDecompressor decompressor2 = factory.safeDecompressor();
    int decompressedLength2 = decompressor2.decompress(compressed, 0, compressedLength, restored, 0);
    // decompressedLength == decompressedLength2
    
    
      System.out.println(new String(compressed,0,compressedLength)+"->"+decompressedLength+"->"+compressedLength+"->"+compressedLength2+"->"+decompressedLength2);
    
    
  }

}
