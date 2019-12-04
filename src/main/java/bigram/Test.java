package bigram;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Test {

  public static void main(String[] args) {
//    String line = "I know not what he shall. God send him well!";
//    line = line.replaceAll("[,.()*'\\[\\]!?]", "");
//    System.out.println(line);
//
//    for (String word : line.split(" ")) {
//      String bigram;
//      if (word.length() == 1) {
//        bigram = word;
//      }
//      for (int i = 0; i < word.length() - 1; i++) {
//        char[] chars = word.toCharArray();
//        bigram = chars[i] + "" + chars[i + 1];
//        System.out.println(bigram);
//      }
//    }


    Integer[] arr = new Integer[]{2,3,4,1,9,8};
    List<Integer> list =  Arrays.asList(arr);
    Collections.sort(list, new Comparator<Integer>() {
      public int compare(Integer o1, Integer o2) {
        return o2-o1;
      }
    });
    for (int i : list) {
      System.out.println(i);
    }
  }

}
