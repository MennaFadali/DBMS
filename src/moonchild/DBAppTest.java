package moonchild;

import java.io.IOException;
import java.util.Hashtable;

public class DBAppTest {
    public static void main(String[] args) throws DBAppException, InterruptedException, IOException {
        DBApp database = new DBApp();
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        database.createTable(strTableName, "id", htblColNameType);
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new Integer(1));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(0.95));
        database.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(5674567));
//        htblColNameValue.put("name", new String("Dalia Noor"));
//        htblColNameValue.put("gpa", new Double(1.25));
//        Thread.sleep(3000);
//        database.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(3));
        htblColNameValue.put("name", new String("Menna Fadali"));
        htblColNameValue.put("gpa", new Double(1.25));
        Thread.sleep(2000);
        database.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(2));
        htblColNameValue.put("name", new String("John Noor"));
        htblColNameValue.put("gpa", new Double(1.5));
        Thread.sleep(1000);
//        database.updateTable(strTableName, 99674567, htblColNameValue);
        database.insertIntoTable(strTableName, htblColNameValue);
    }
}