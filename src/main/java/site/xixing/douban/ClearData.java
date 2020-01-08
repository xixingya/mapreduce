package site.xixing.douban;

import java.io.*;

/**
 * @author xixing
 * @version 1.0
 * @date 2020/1/5 10:03
 */
public class ClearData {
    public static void main(String[] args) throws IOException {
        BufferedReader br=new BufferedReader(new FileReader("D:\\xiaoshuo.csv"));
        BufferedWriter bw=new BufferedWriter(new FileWriter("D:\\xiaoshuoc.csv"));
        String str;
        while ((str=br.readLine())!=null){
            if(str.length()>1&&(str.charAt(str.length()-1)=='元'||str.charAt(str.length()-1)=='0')){
                bw.write(str+"\n");
            }else {
                bw.write(str);
            }
        }
//        BufferedReader br=new BufferedReader(new FileReader("D:\\xiaoshuoc.txt"));
//        BufferedWriter bw=new BufferedWriter(new FileWriter("D:\\xiaoshuofinal.txt"));
//        String str;
//
//        /*while((str=br.readLine())!=null){
//            bw.write(s);
//        }*/
        bw.close();
        br.close();
    }
}
