package moonchild;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class BitMapIndex {
    String tableName, colName;
    HashMap<Object, BitMap> colValues;

    BitMapIndex(Table table, String colName) {
        tableName = table.tablename;
        this.colName = colName;
        colValues = new HashMap<>();
        int idx = 0;
        for (Page page : table.pages) {
            for (HashMap<String, Object> hm : page.tuples) {
                Object value = hm.get(colName);
                if (!colValues.containsKey(value)) colValues.put(value, new BitMap(table.size));
                colValues.get(value).set(idx++);
            }
        }
    }

    //Adding a new tuple int he table will result that we have to change all bitmap's indices size
    void AddTuple(int idx) {
        for (Object value : colValues.keySet())
            colValues.get(value).addBitAfter(idx, 0);

    }

    void saveIndex() {
        String path = "/data/" + tableName + colName;
        try {
            FileWriter fileWriter = new FileWriter(path + "0");
            int cnt = 0;
            int p = 0;
            for (Object value : colValues.keySet()) {
                if (cnt == DBApp.M) {
                    p++;
                    cnt = 0;
                    fileWriter.flush();
                    fileWriter.close();
                    fileWriter = new FileWriter(path + p);
                }
                cnt++;
                fileWriter.write(value + "," + colValues.get(value).toString() + "\n");
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
