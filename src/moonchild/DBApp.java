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

    public void getAllTables() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(metadata)));
            while (br.ready()) {
                String[] line = br.readLine().split(",");
                tables.add(line[0]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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

    //Cecking for the datatype of the column ?
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
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
        Table.saveTable(res);
        return;

    }


    public void updateTable(String strTableName, Object strKey, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        //Handling the Exceptions
        /*
        Check that the Hashtable is correct
         */
        if (!tables.contains(strTableName)) throw new DBAppException("The Table does not exist");
        String primary = getClusteringColumn(strTableName);
        if (primary.equals("Not Found")) throw new DBAppException("The Table does not exist");
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
        Table.saveTable(cur);
    }


    //Checking for if the primary key existed before ?
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Table cur = Table.loadTable(strTableName);
        HashMap<String, String> types = getDatatypes(strTableName);
        for (String col : htblColNameValue.keySet()) {
            if (!types.containsKey(col))
                throw new DBAppException("Sorry this table does not have the coloumn " + col);
            if (!types.get(col).equals(htblColNameValue.get(col).getClass().toString()))
                throw new DBAppException("Sorry the coloumn " + col + " does not have this datatype");
        }
        String primarycol = getClusteringColumn(strTableName);
        String primarytype = getClusteringColumnTyple(strTableName);
        HashMap<String, Object> insert = new HashMap<>();
        for (String col : htblColNameValue.keySet())
            insert.put(col, htblColNameValue.get(col));
        insert.put("TouchDate", (new Date()));
        Table res = new Table(strTableName);
        for (Page page : cur.pages) {
            Page rpage = new Page(strTableName + res.pages.size());
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
                Page tmp = new Page(strTableName + cur.pages.size());
                tmp.tuples.add(insert);
                res.pages.add(tmp);
            } else
                res.pages.get(res.pages.size() - 1).tuples.add(insert);

        }
        res.arr = cur.arr;
        Table.saveTable(res);

    }

    public HashMap<String, String> getDatatypes(String tablename) {
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

        return null;
    }


}