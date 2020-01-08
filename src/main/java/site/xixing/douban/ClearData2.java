package site.xixing.douban;

import java.io.*;

/**
 * @author xixing
 * @version 1.0
 * @date 2020/1/7 16:35
 */
public class ClearData2 {
    public static void main(String[] args) throws IOException {
        BufferedReader br=new BufferedReader(new FileReader("D:\\datas\\jiaohusheji.csv"));
        BufferedWriter bw=new BufferedWriter(new FileWriter("D:\\datas\\jiaohushejic.csv"));
        String str;
        while((str=br.readLine())!=null){
            if(str.length()>3){
                bw.write(str+"\n");
            }

        }
        bw.close();
        br.close();

    }

}
