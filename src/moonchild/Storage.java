package moonchild;

import java.io.*;
import java.util.HashMap;

public class Storage {
    HashMap<String, HashMap<Integer, Integer>> reference;

    //TableName,PageNum,idxofTheFirstElemnt
    public Storage() {
        reference = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("/data/metadatastorage.csv")));
            while (br.ready()) {
                String Line[] = br.readLine().split(",");
                if (!reference.containsKey(Line[0])) reference.put(Line[0], new HashMap<>());
                reference.get(Line[0]).put(Integer.parseInt(Line[1]), Integer.parseInt(Line[2]));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    void SaveStorage() {
        try {
            FileWriter fw = new FileWriter("/data/metadatastorage.csv");
            for (String tablename : reference.keySet())
                for (int page : reference.get(tablename).keySet())
                    fw.write(tablename + "," + page + "," + reference.get(tablename).get(page) + "\n");
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
