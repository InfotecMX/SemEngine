/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.xerial.snappy.Snappy;

/**
 *
 * @author javiersolis
 */
public class TestSnappy {

    public static void main(String args[])
    {
        try {
            String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of Snappy, a fast compresser/decompresser.";
            byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
            byte[] uncompressed = Snappy.uncompress(compressed);

            String result = new String(compressed, "UTF-8");
            System.out.println(uncompressed.length+"->"+compressed.length+"->"+result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
