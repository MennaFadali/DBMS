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
                if (!colValues.containsKey(value))
                    colValues.put(DBApp.convert(value), new BitMap(table.size));
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
                    colValues.put(value, new BitMap(decompress(line[1])));
//                    colValues.put(value, new BitMap((line[1])));
                }
                p++;
            } catch (FileNotFoundException e) {
                break;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    static String compress(String index) {
        char[] c = index.toCharArray();
        String res = "";
        int cnt = 1;
        for (int i = 1; i < c.length; ++i) {
            if (c[i] == c[i - 1])
                cnt++;
            else {
                res += c[i - 1] + "#" + cnt + "#";
                cnt = 1;
            }
        }
        res += c[c.length - 1] + "#" + cnt;
        return res;
    }

    static String decompress(String cmp) {
        String res = "";
        String[] s = cmp.split("#");
        for (int i = 1; i < s.length; i += 2) {
            int freq = Integer.parseInt(s[i]);
            while (freq > 0) {
                freq--;
                res += s[i - 1];
            }
        }
        return res;
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
                fileWriter.write(value + "," + (compress(colValues.get(value).toString())) + "\n");
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
                        String indexname = tableName + col;
                        Comparable oldvalue = (Comparable) tuple.get(col);
                        tuple.put(col, htblColNameValue.get(col));
                        if (!DBApp.indices.containsKey(indexname))
                            continue;
                        TreeMap<Comparable, BitMap> bIdx = DBApp.indices.get(indexname).colValues;
                        Comparable newval = (Comparable) htblColNameValue.get(col);
                        bIdx.get(oldvalue).clear(idx);
                        if (!bIdx.containsKey(newval)) bIdx.put(newval, new BitMap(bIdx.get(oldvalue).bits.size()));
                        bIdx.get(newval).set(idx);
                    }
                }
            }
            Page.savePage(cur, arr);
        }
    }

    BitMap getTheBitMap(String operator, Comparable value) {
        int n = (colValues.get(colValues.lastKey())).bits.size();
        switch (operator) {
            case "=":
                return colValues.get(value);
            case "!=":
                return colValues.containsKey(value) ? BitMap.not(colValues.get(value)) : new BitMap(n, '1');
            case "<":
                return getLow(colValues.lowerKey(value), n);
            case "<=":
                return getLow(value, n);
            case ">":
                return getHigh(colValues.higherKey(value), n);
            case ">=":
                return getHigh(value, n);
        }
        return null;
    }

    BitMap getLow(Comparable val, int n) {
        BitMap ans = new BitMap(n);
        while (val != null) {
            ans = BitMap.or(ans, colValues.get(val));
            val = colValues.lowerKey(val);
        }
        return ans;
    }

    BitMap getHigh(Comparable val, int n) {
        BitMap ans = new BitMap(n);
        while (val != null) {
            ans = BitMap.or(ans, colValues.get(val));
            val = colValues.higherKey(val);
        }
        return ans;
    }

    //update all BITMAPsss on this table!!!
    void insert(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // equality and comparability
        Comparable v = (Comparable) htblColNameValue.get(colName);
        Comparable less = colValues.floorKey(v);
        Storage st = DBApp.pageformation;
        int pageCount = st.reference.get(tableName).size();
        int startPage = 0;
        if (less != null) {
            BitMap idxLess = colValues.get(less);
            int n = idxLess.bits.size();
            for (int i = 0; i < n; ++i)
                if (idxLess.get(i) == 1) {
                    startPage = i;
                    break;
                }
        }
        //startpage => Index in the bitmap lying in the startpage
        HashMap<String, Object> insert = new HashMap<>();
        for (String col : htblColNameValue.keySet())
            insert.put(col, htblColNameValue.get(col));
        insert.put("TouchDate", (new Date()));
        HashMap<Integer, Integer> lastTuplePerPage = st.reference.get(tableName);
        int increasedIndex = pageCount;
        String[] arr = Table.getArrangements(tableName);
        HashMap<String, String> types = Table.getArrangementType(tableName);
        int firstInsert = -1;
        Page begin = st.getPagetofTupleNumber(tableName, startPage);
        int realStart = begin.number;
        for (int i = realStart; i < pageCount; i++) {
            if (insert == null)
                break;
            Page cur = Page.loadPage(tableName + i, i, arr, types);//st.getPagetofTupleNumber(tableName, i);
            Page rpage = new Page(cur.name, i);
            if (cur.tuples.size() < DBApp.N)
                increasedIndex = i;
            for (int j = 0; j < cur.tuples.size(); ++j) {
                HashMap<String, Object> hm = cur.tuples.get(j);
                boolean flag = DBApp.compare((Comparable) hm.get(colName), (Comparable) insert.get(colName));
                if (flag) {
                    if (firstInsert == -1)
                        firstInsert = j + (i > 0 ? lastTuplePerPage.get(i - 1) : 0);
                    rpage.tuples.add(insert);
                    insert = null;
                }
                rpage.tuples.add(hm);
            }
            if (i == pageCount - 1 && insert != null && rpage.tuples.size() < DBApp.N) {
                rpage.tuples.add(insert);
                insert = null;
                if (firstInsert == -1)
                    firstInsert = lastTuplePerPage.get(pageCount - 1);
            }
            if (rpage.tuples.size() > DBApp.N)
                insert = rpage.tuples.remove(DBApp.N);
            rpage.savePage(rpage, arr);
        }
        for (int i = increasedIndex; i < pageCount; ++i) {
            lastTuplePerPage.put(i, lastTuplePerPage.get(i) + 1);
        }
        if (insert != null) {
            Page New = new Page(tableName + pageCount, pageCount);
            New.tuples.add(insert);
            New.savePage(New, arr);
            Table.saveArrangements(tableName, arr, pageCount + 1);
            lastTuplePerPage.put(pageCount, lastTuplePerPage.get(pageCount - 1) + 1);
            if (firstInsert == -1)
                firstInsert = lastTuplePerPage.get(pageCount) - 1;
            pageCount++;
        }
        for (String index : DBApp.indices.keySet()) {
            if (index.length() < tableName.length() || !index.substring(0, tableName.length()).equals(tableName))
                continue;
            TreeMap<Comparable, BitMap> bIdx = DBApp.indices.get(index).colValues;
            for (Comparable val : bIdx.keySet()) {
                BitMap cur = bIdx.get(val);
                cur.addBitAfter(firstInsert - 1, 0);
            }
        }
        for (String colName : htblColNameValue.keySet()) {
            String indexname = tableName + colName;
            if (DBApp.indices.containsKey(indexname)) {
                TreeMap<Comparable, BitMap> bIdx = DBApp.indices.get(indexname).colValues;
                if (bIdx.containsKey(htblColNameValue.get(colName)))
                    bIdx.get(htblColNameValue.get(colName)).set(firstInsert);
                else {
                    int n = lastTuplePerPage.get(pageCount - 1);
                    BitMap tmp = new BitMap(n);
                    tmp.set(firstInsert);
                    bIdx.put((Comparable) htblColNameValue.get(colName), tmp);
                }
            }

        }
        BitMap created = new BitMap(lastTuplePerPage.get(pageCount - 1));
        created.set(firstInsert);
        colValues.put(v, created);
    }

    void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Comparable v = (Comparable) htblColNameValue.get(colName);
        BitMap m = colValues.get(v);
        String clone = m.toString();
        for (int i = 0; i < clone.length(); ++i) {
            if (clone.charAt(i) == '1') {
                deleteTuple(i, htblColNameValue);
            }
        }
    }

    void deleteTuple(int idx, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Storage st = DBApp.pageformation;
        String[] arr = Table.getArrangements(tableName);
        Page page = st.getPagetofTupleNumber(tableName, idx);
        Page rpage = new Page(page.name, page.number);
        boolean firstDel = true;
        for (HashMap<String, Object> hm : page.tuples) {
            boolean del = true;
            for (String x : htblColNameValue.keySet()) {
                if (!hm.get(x).equals(htblColNameValue.get(x))) {
                    del = false;
                }
            }

            if (!del || !firstDel)
                rpage.tuples.add(hm);
            if (del) firstDel = false;
        }
        //Handle if the page is empty
        HashMap<Integer, Integer> lastTuplePerPage = st.reference.get(tableName);
        int pageCount = lastTuplePerPage.size();
        HashMap<String, String> types = Table.getArrangementType(tableName);

        if (rpage.tuples.isEmpty()) {
            for (int i = page.number + 1; i < pageCount; ++i) {
                Page move = Page.loadPage(tableName + i, i, arr, types);
                move.name = tableName + (i - 1);
                move.number = i - 1;
                Page.savePage(move, arr);
            }
            //remove last page from disk
            File f = new File("data/" + tableName + (pageCount - 1));
            f.delete();
        } else {
            Page.savePage(rpage, arr);
        }
        for (String index : DBApp.indices.keySet()) {
            if (index.length() < tableName.length() || !index.substring(0, tableName.length()).equals(tableName))
                continue;
            TreeMap<Comparable, BitMap> bIdx = DBApp.indices.get(index).colValues;
            for (Comparable val : bIdx.keySet()) {
                BitMap cur = bIdx.get(val);
                cur.removeBit(idx);
            }
        }
        for (int i = page.number; i < pageCount; ++i) {
            lastTuplePerPage.put(i, Math.max(0, lastTuplePerPage.get(i) - 1));
        }
    }

    @Override
    public String toString() {
        return tableName + " " + colName + " " + colValues;
    }

}
