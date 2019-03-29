package moonchild;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

public class DBAppTest {
    public static void main(String[] args) throws DBAppException, InterruptedException, IOException {
        DBApp database = new DBApp();
//        HashMap<String, Object> htblColNameValue = new HashMap<>();
        String strTableName = "Student";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
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
//        htblColNameValue.put("name", new String("A"));
//        htblColNameValue.put("gpa", new Double(0.95));
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
//        htblColNameValue.put("id", new Integer(6));
//        database.deleteFromTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(0));
        htblColNameValue.put("name", new String("Y"));
        htblColNameValue.put("gpa", new Double(0.85));
        database.insertIntoTable(strTableName, htblColNameValue);
//        SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[3];
//        for (int i = 0; i < 3; i++)
//            arrSQLTerms[i] = new SQLTerm();
//        arrSQLTerms[0]._strTableName = "Student";
//        arrSQLTerms[0]._strColumnName = "name";
//        arrSQLTerms[0]._strOperator = "<=";
//        arrSQLTerms[0]._objValue = "D";
//        arrSQLTerms[1]._strTableName = "Student";
//        arrSQLTerms[1]._strColumnName = "gpa";
//        arrSQLTerms[1]._strOperator = ">=";
//        arrSQLTerms[1]._objValue = new Double(0.7);
//        arrSQLTerms[2]._strTableName = "Student";
//        arrSQLTerms[2]._strColumnName = "id";
//        arrSQLTerms[2]._strOperator = "<";
//        arrSQLTerms[2]._objValue = new Integer(8);
//        String[] strarrOperators = new String[2];
//        strarrOperators[0] = "AND";
//        strarrOperators[1] = "AND";
////        System.err.println(database.satisfies(htblColNameValue, arrSQLTerms, strarrOperators));
//        Iterator i = database.selectFromTable(arrSQLTerms, strarrOperators);
//        while (i.hasNext())
//            System.err.println(i.next());
//        database.createBitmapIndex("Student", "gpa");
        database.closeApp();
    }

}