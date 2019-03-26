package moonchild;

import java.io.*;
import java.util.*;

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


    void updateTableWithIndex(Comparable strKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (!colValues.containsKey(strKey)) return;
        BitMap primarybitmap = colValues.get(strKey);
        ArrayList<Integer> idxs = primarybitmap.findOnes();
        String[] arr = Table.getArrangements(tableName);
        for (int idx : idxs) {
            Page cur = DBApp.pageformation.getPagetofTupleNumber(tableName, idx);
            for (HashMap<String, Object> tuple : cur.tuples) {
                if (tuple.get(colName).equals(strKey)) {
                    for (String col : htblColNameValue.keySet()) {
                        Comparable oldvalue = (Comparable) tuple.get(col);
                        Comparable newval = (Comparable) htblColNameValue.get(col);
                        colValues.get(oldvalue).clear(idx);
                        colValues.get(newval).set(idx);
                        tuple.put(col, htblColNameValue.get(col));
                    }
                }
            }
            Page.savePage(cur, arr);
        }
    }

    BitMap getTheBitMap(String operator, Object value) {
        BitMap ans = null;
        int n = colValues.get(colValues.lastKey()).bits.size();
        switch (operator) {
            case "=":
                return colValues.get(value);
            case "!=":
                return colValues.containsKey(value) ? BitMap.not(colValues.get(value)) : new BitMap(n, '1');
            case "<":
                return colValues.firstKey().equals(value) ? new BitMap(n) : getOr(colValues.subMap(colValues.firstKey(), colValues.lowerKey((Comparable) value)), n);
            case "<=":
                return  getOr(colValues.subMap(colValues.firstKey(), colValues.lowerKey((Comparable) value)), n);
            case ">" :
//                return colValues.lastKey().equals(value) ? new BitMap(n) : getOr(colValues.subMap(colValues.higherKey((Comparable) value), colValues.lastKey(), n));
            case ">=" :


        }

        return null;
    }

    BitMap getOr(SortedMap<Comparable, BitMap> tm, int n) {
        BitMap ans = new BitMap(n);
        for (Comparable val : tm.keySet()) {
            ans = BitMap.or(ans, tm.get(val));
        }
        return ans;
    }

}
