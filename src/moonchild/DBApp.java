package moonchild;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {
    static transient final String metadata = "data/metadata.csv";
    static transient final String metadatastorage = "data/metadatastorage.csv";
    static transient final DateFormat dateformat = new SimpleDateFormat("E MMM d HH:mm:ss z yyyy");
    // SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    static transient HashSet<String> tables;
    // N represents the maximumNumberofRowsperPage
    static int N;
    //M represents the maximum number of colValue,BitMap per page
    static int M;
    static transient HashMap<String, BitMapIndex> indices;
    static transient Storage pageformation;

    public DBApp() {
        tables = new HashSet<>();
        Properties prop = new Properties();
        indices = new HashMap<>();
        pageformation = new Storage();
        getIndicesFromMetaDataFile();
        getAllTables();
        try {
            prop.load(new FileInputStream("config/DBApp.properties"));
            this.N = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
            this.M = Integer.parseInt(prop.getProperty("BitmapSize"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void CSV(Hashtable<String, String> h, String name, String key) throws moonchild.DBAppException {
        Set entrySet = h.entrySet();
        Iterator it = entrySet.iterator();
        FileWriter fw = null;
        try {
            fw = new FileWriter("data/metadata.csv");
            while (it.hasNext()) {
                String s = it.next() + "";
                StringTokenizer sp = new StringTokenizer(s, "=");
                String colname = sp.nextToken();
                String coltype = sp.nextToken();
                fw.append(name);
                fw.append(",");
                fw.append(colname);
                fw.append(",");
                fw.append(coltype);
                fw.append(",");
                if (key.equals(colname))
                    fw.append("true");
                else
                    fw.append("false");
                fw.append(",");
                fw.append("false");
                fw.append("\n");
            }
            fw.flush();
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String getClusteringColumn(String tablename) {
        try {
            BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
            while (br.ready()) {
                String[] line = br.readLine().split(",");
                if (!line[0].equals(tablename))
                    continue;
                if (line[3].equals("true"))
                    return line[1];
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Not Found";
    }

    static String getClusteringColumnTyple(String tablename) {
        try {
            BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
            while (br.ready()) {
                String[] line = br.readLine().split(",");
                if (!line[0].equals(tablename))
                    continue;
                if (line[3].equals("true"))
                    return line[2];
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    static String getObjectType(Object a) {
        return a.getClass().toString().substring(6);
    }

    static Comparable convert(Object a) {
        return (Comparable) a;
    }

    static boolean hasIndex(String tablename, String colName) {
        return indices.containsKey(tablename + colName);
    }

    static Comparable convert(String a, String type) {
        switch (type) {
            case "java.lang.double":
            case "java.lang.Double":
                return Double.parseDouble(a);
            case "java.lang.Integer":
                return Integer.parseInt(a);
            case "java.lang.String":
                return a;
            case "java.util.Date":
                try {
                    return dateformat.parse(a);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            case "java.lang.Boolean":
                return Boolean.parseBoolean(a);

        }
        return null;
    }

    static boolean compare(Comparable a, Comparable b) {
        return a.compareTo(b) == 1;
    }

    static boolean satisfies(HashMap<String, Object> hm, SQLTerm[] terms, String[] operations) {
        Chain chain = new Chain();
        int n = terms.length;
        for (int i = n - 1; i > 0; i--) {
            chain.addFirst(terms[i]);
            chain.addFirst(operations[i - 1]);
        }
        chain.addFirst(terms[0]);
        chain.resolveAnd(hm);
        chain.resolveXOR(hm);
        chain.resolveOr(hm);
        return (boolean) chain.head.value;
    }

    static boolean satisfies(HashMap<String, Object> hm, Object term) {
        if (term instanceof Boolean) return (boolean) term;
        SQLTerm t = (SQLTerm) term;
        Comparable curval = (Comparable) hm.get(t._strColumnName);
        switch (t._strOperator) {
            case "=":
                return curval.equals(t._objValue);
            case "!=":
                return !curval.equals(t._objValue);
            case "<":
                return curval.compareTo(t._objValue) < 0;
            case "<=":
                return curval.compareTo(t._objValue) <= 0;
            case ">":
                return curval.compareTo(t._objValue) > 0;
            case ">=":
                return curval.compareTo(t._objValue) >= 0;
        }
        return false;
    }

    public static HashMap<String, String> getDatatypes(String tablename) {
        FileReader fr;
        HashMap<String, String> ans = new HashMap<>();
        try {
            fr = new FileReader("data/metadata.csv");
            BufferedReader br = new BufferedReader(fr);
            while (br.ready()) {
                String[] tmp = br.readLine().split(",");
                if (!tmp[0].equals(tablename))
                    continue;
                ans.put(tmp[1], "class " + tmp[2]);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ans;
    }

    public static void exceptionCheck(String tableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (!tables.contains(tableName)) throw new DBAppException("The Table does not exist");
        HashMap<String, String> types = getDatatypes(tableName);
        for (String col : htblColNameValue.keySet()) {
            if (!types.containsKey(col))
                throw new DBAppException("Sorry this table does not have the coloumn " + col);
            if (!types.get(col).equals(htblColNameValue.get(col).getClass().toString()))
                throw new DBAppException("Sorry the coloumn " + col + " does not have this datatype");
        }

    }

    static BitMap satisfies(BitMap[] bitMaps, String[] operations) {
        Chain chain = new Chain();
        int n = bitMaps.length;
        for (int i = n - 1; i > 0; i--) {
            chain.addFirst(bitMaps[i]);
            chain.addFirst(operations[i - 1]);
        }
        chain.addFirst(bitMaps[0]);
        chain.resolveAnd();
        chain.resolveXOR();
        chain.resolveOr();
        return (BitMap) chain.head.value;
    }

    public void getAllTables() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(metadata)));
            while (br.ready()) {
                String[] line = br.readLine().split(",");
                tables.add(line[0]);
            }
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void closeApp() {
        pageformation.SaveStorage();
        for (String index : indices.keySet())
            indices.get(index).saveIndex();
    }

    void getIndicesFromMetaDataFile() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(metadata)));
            while (br.ready()) {
                String line[] = br.readLine().split(",");
                if (line[4].equals("true")) {
                    String tablename = line[0];
                    String colName = line[1];
                    indices.put(tablename + colName, new BitMapIndex(tablename, colName, line[2]));
                }
            }
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String strTableName, String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType) throws DBAppException {
        if (tables.contains(strTableName))
            throw new DBAppException("This table already exists in the DataBase");
        tables.add(strTableName);
        htblColNameType.put("TouchDate", "java.util.Date");
        Table.saveArrangements(strTableName, htblColNameType, 0);
        CSV(htblColNameType, strTableName, strClusteringKeyColumn);
    }

    public void updateTable(String strTableName, Object strKey, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        exceptionCheck(strTableName, htblColNameValue);
        String primary = getClusteringColumn(strTableName);
        String type = getClusteringColumnTyple(strTableName);
        if (!type.equals(getObjectType(strKey)))
            throw new DBAppException("The type of the given primary key does not match the type of the clustering key of the table");
        if (hasIndex(strTableName, primary)) {
            indices.get(strTableName + primary).updateTableWithIndex(convert(strKey), htblColNameValue);
            return;
        }
        Table cur = Table.loadTable(strTableName);
        search:
        for (Page p : cur.pages) {
            for (HashMap<String, Object> tuple : p.tuples) {
                if (tuple.get(primary).equals(strKey)) {
                    for (String col : htblColNameValue.keySet()) {
                        if (!tuple.containsKey(col))
                            throw new DBAppException("Not a valid coloumn name: " + col);
                        if (!tuple.get(col).getClass().toString()
                                .equals(htblColNameValue.get(col).getClass().toString()))
                            throw new DBAppException("Not a valid type for the coloumn: " + col);
                        tuple.put(col, htblColNameValue.get(col));
                    }
                    tuple.put("TouchDate", dateformat.format(new Date()));
                    break search;
                }

            }
        }
        pageformation.UpdateTable(cur);
        Table.saveTable(cur);
    }

    //Cecking for the datatype of the column ?
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        exceptionCheck(strTableName, htblColNameValue);
        for (String col : htblColNameValue.keySet()) {
            if (hasIndex(strTableName, col)) {
                indices.get(strTableName + col).delete(htblColNameValue);

                return;
            }
        }
        Table cur = Table.loadTable(strTableName);
        Table res = new Table(strTableName);
        HashMap<String, String> types = getDatatypes(strTableName);
        for (String col : htblColNameValue.keySet()) {
            if (!types.containsKey(col))
                throw new DBAppException("Sorry this table does not have the coloumn " + col);
            if (!types.get(col).equals(htblColNameValue.get(col).getClass().toString()))
                throw new DBAppException("Sorry the coloumn " + col + " does not have this datatype");
        }
        for (Page p : cur.pages) {
            Page rpage = new Page();
            for (HashMap<String, Object> hm : p.tuples) {
                boolean del = true;
                for (String x : htblColNameValue.keySet()) {
                    if (!hm.get(x).equals(htblColNameValue.get(x)))
                        del = false;
                }
                if (!del)
                    rpage.tuples.add(hm);
            }
            rpage.setName(strTableName + res.pages.size());
            if (rpage.tuples.size() != 0)
                res.pages.add(rpage);
        }
        res.arr = cur.arr;
        pageformation.UpdateTable(res);
        Table.saveTable(res);
        managesIndicies(strTableName);
        return;

    }

    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        exceptionCheck(strTableName, htblColNameValue);
        String primarycol = getClusteringColumn(strTableName);
        if (hasIndex(strTableName, primarycol)) {
            indices.get(strTableName + primarycol).insert(htblColNameValue);
            return;
        }
        Table cur = Table.loadTable(strTableName);
        HashMap<String, String> types = getDatatypes(strTableName);
        for (String col : htblColNameValue.keySet()) {
            if (!types.containsKey(col))
                throw new DBAppException("Sorry this table does not have the coloumn " + col);
            if (!types.get(col).equals(htblColNameValue.get(col).getClass().toString()))
                throw new DBAppException("Sorry the coloumn " + col + " does not have this datatype");
        }
        HashMap<String, Object> insert = new HashMap<>();
        for (String col : htblColNameValue.keySet())
            insert.put(col, htblColNameValue.get(col));
        insert.put("TouchDate", (new Date()));
        Table res = new Table(strTableName);
        for (Page page : cur.pages) {
            Page rpage = new Page(strTableName + res.pages.size(), res.pages.size());
            for (HashMap<String, Object> hm : page.tuples) {
                if (insert != null) {
                    boolean flag = compare((Comparable) hm.get(primarycol), (Comparable) insert.get(primarycol));
                    if (flag) {
                        rpage.tuples.add(insert);
                        insert = null;
                    }
                }
                rpage.tuples.add(hm);
            }
            if (rpage.tuples.size() > N)
                insert = rpage.tuples.remove(N);
            res.pages.add(rpage);
        }
        if (insert != null) {
            if (cur.pages.size() == 0 || cur.pages.get(cur.pages.size() - 1).tuples.size() == N) {
                Page tmp = new Page(strTableName + cur.pages.size(), cur.pages.size());
                tmp.tuples.add(insert);
                res.pages.add(tmp);
            } else
                res.pages.get(res.pages.size() - 1).tuples.add(insert);

        }
        res.arr = cur.arr;
        pageformation.UpdateTable(res);
        Table.saveTable(res);
        managesIndicies(strTableName);
    }

    /*
    ##########################################################
                    PART TWO OF THE PROJECT
    ##########################################################
     */
    void updateMetaDataCSVFile(String tableName, String colName) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(metadata)));
            StringBuilder sb = new StringBuilder();
            while (br.ready()) {
                String line[] = br.readLine().split(",");
                if (line[0].equals(tableName) && line[1].equals(colName)) line[4] = "true";
                for (int i = 0; i < 5; i++)
                    sb.append(line[i] + (i == 4 ? "" : ","));
                sb.append("\n");
            }
            FileWriter fw = new FileWriter(metadata);
            fw.write(sb.toString());
            fw.flush();
            fw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createBitmapIndex(String strTableName, String strColName) throws DBAppException {
        Table table = Table.loadTable(strTableName);
        indices.put(strTableName + strColName, new BitMapIndex(table, strColName));
        updateMetaDataCSVFile(strTableName, strColName);
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        ArrayList<HashMap<String, Object>> resultset = new ArrayList<>();
        String tablename = arrSQLTerms[0]._strTableName;
        boolean allhasindex = true;
        for (SQLTerm term : arrSQLTerms)
            if (!DBApp.indices.containsKey(tablename + term._strColumnName)) allhasindex = false;
        if (!allhasindex) {
            Table table = Table.loadTable(tablename);
            for (Page cur : table.pages)
                for (HashMap<String, Object> tuple : cur.tuples)
                    if (satisfies(tuple, arrSQLTerms, strarrOperators)) resultset.add(tuple);
        } else {
            BitMap[] bitmaps = new BitMap[arrSQLTerms.length];
            for (int i = 0; i < arrSQLTerms.length; i++)
                bitmaps[i] = indices.get(tablename + arrSQLTerms[i]._strColumnName).getTheBitMap(arrSQLTerms[i]._strOperator, (Comparable) arrSQLTerms[i]._objValue);
            HashSet<Integer> loadedPages = new HashSet<>();
            BitMap satisfiesall = satisfies(bitmaps, strarrOperators);
            ArrayList<Integer> idxs = satisfiesall.findOnes();
            for (int idx : idxs) {
                int curnum = pageformation.getPageNumbertofTupleNumber(tablename, idx);
                if (loadedPages.contains(curnum)) continue;
                loadedPages.add(curnum);
                Page cur = pageformation.getPagetofTupleNumber(tablename, idx);
                for (HashMap<String, Object> tuple : cur.tuples)
                    if (satisfies(tuple, arrSQLTerms, strarrOperators)) resultset.add(tuple);

            }
        }
        return resultset.iterator();
    }

    public void managesIndicies(String tableName) throws DBAppException {
        HashSet<String> remove = new HashSet<>();
        for (String index : indices.keySet()) {
            if (index.length() < tableName.length() || !index.substring(0, tableName.length()).equals(tableName))
                continue;
            remove.add(index);
        }
        for (String index : remove) {
            indices.remove(index);
            this.createBitmapIndex(tableName, index.substring(tableName.length()));
        }
    }

}