
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author javiersolis
 */
public class TestSort {
    public static void main(String args[])
    {
        Comparator comp=new Comparator<String>(){
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };        
        
        long time=System.currentTimeMillis();
        Random rand=new Random();
        ArrayList arr=new ArrayList(500000);
        //TreeSet arr=new TreeSet(comp);
        for(int x=0;x<500000;x++)
        {
            StringBuilder sb=new StringBuilder();
            for(int i=0;i<100;i++)
            {
                sb.append(('A'+rand.nextInt(32)));
            }
            arr.add(sb.toString());
        }
        System.out.println("time:"+(System.currentTimeMillis()-time));
        
        
//        TreeSet set=new TreeSet(comp);
//        Iterator<String> it=arr.iterator();
//        time=System.currentTimeMillis();
//        while (it.hasNext()) {
//            set.add(it.next());
//        }
//        System.out.println("time:"+(System.currentTimeMillis()-time));
//        
        time=System.currentTimeMillis();
        Collections.sort(arr);
        System.out.println("time:"+(System.currentTimeMillis()-time));
        
//        time=System.currentTimeMillis();
//        List<String> out=(List)arr.stream().sorted(comp).collect(Collectors.toList());
//        System.out.println("time:"+(System.currentTimeMillis()-time));
        
    }
}
