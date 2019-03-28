package moonchild;

import java.io.IOException;
import java.util.Hashtable;

public class DBAppTest {
    public static void main(String[] args) throws DBAppException, InterruptedException, IOException {
        DBApp database = new DBApp();
        Hashtable htblColNameValue = new Hashtable();
        String strTableName = "Student";
//        Hashtable htblColNameType = new Hashtable();
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.Double");
//        database.createTable(strTableName, "id", htblColNameType);
//        htblColNameValue.put("id", new Integer(1));
//        htblColNameValue.put("name", new String("A"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        database.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(2));
//        htblColNameValue.put("name", new String("B"));
//        htblColNameValue.put("gpa", new Double(1.25));
//        database.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(3));
//        htblColNameValue.put("name", new String("A"));
//        htblColNameValue.put("gpa", new Double(1.25));
//        database.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(4));
//        htblColNameValue.put("name", new String("D"));
//        htblColNameValue.put("gpa", new Double(1.5));
//        database.insertIntoTable(strTableName, htblColNameValue);
//        database.createBitmapIndex("Student", "name");
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(8));
//        htblColNameValue.put("name", new String("D"));
//        htblColNameValue.put("gpa", new Double(0.7));
//        database.insertIntoTable(strTableName, htblColNameValue);
//        database.createBitmapIndex("Student", "id");
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(6));
//        htblColNameValue.put("name", new String("Y"));
//        htblColNameValue.put("gpa", new Double(0.7));
//        database.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("name", new String("B"));
//        htblColNameValue.put("gpa", new Double(4.45));
//        database.updateTable(strTableName, new Integer(6), htblColNameValue);
//        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(6));
        database.deleteFromTable(strTableName, htblColNameValue);
        database.closeApp();
    }

}