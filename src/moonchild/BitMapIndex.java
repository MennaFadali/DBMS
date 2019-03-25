package moonchild;

import java.io.*;
import java.util.HashMap;
import java.util.TreeMap;

public class BitMapIndex {
    String tableName, colName;
    TreeMap<Comparable, BitMap> colValues;

    BitMapIndex(Table table, String colName) {
        tableName = table.tablename;
        this.colName = colName;
        colValues = new TreeMap<>();
        int idx = 0;
        for (Page page : table.pages) {
            for (HashMap<String, Object> hm : page.tuples) {
                Object value = hm.get(colName);
                if (!colValues.containsKey(value)) colValues.put(DBApp.convert(value), new BitMap(table.size));
                colValues.get(value).set(idx++);
            }
        }
    }

    BitMapIndex(String tableName, String colName, String type) {
        this.tableName = tableName;
        this.colName = colName;
        colValues = new TreeMap<>();
        String indexname = tableName + colName;
        String path = "data/" + indexname;
        int p = 0;
        while (true) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(new File(path + p)));
                while (br.ready()) {
                    String line[] = br.readLine().split(",");
                    Comparable value = DBApp.convert(line[0], type);
                    colValues.put(value, new BitMap(line[1]));
                }
                p++;
            } catch (FileNotFoundException e) {
                break;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    //Adding a new tuple int he table will result that we have to change all bitmap's indices size
    void AddTuple(int idx) {
        for (Object value : colValues.keySet())
            colValues.get(value).addBitAfter(idx, 0);

    }

    void saveIndex() {
        String path = "data/" + tableName + colName;
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
