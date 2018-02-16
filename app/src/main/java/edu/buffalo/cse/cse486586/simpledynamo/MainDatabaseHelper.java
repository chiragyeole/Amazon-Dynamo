package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by chiragyeole on 5/1/17.
 */

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;



public class MainDatabaseHelper extends SQLiteOpenHelper {

    public static final String TAG = MainDatabaseHelper.class.getSimpleName();
    private static final String SQL_CREATE_MAIN = "CREATE TABLE " +
            "MESSENGER " +                       // Table's name
            "(" +                           // The columns in the table
            "key TEXT not null, value TEXT not null,portversion TEXT not null)";
    private static final String DBNAME = "messengerDb";
     static String Recover = "RECOVER";
    /*
     * Instantiates an open helper for the provider's SQLite data repository
     * Do not do database creation and upgrade here.
     */
    MainDatabaseHelper(Context context) {
        super(context, DBNAME, null, 2);
    }

    /*
     * Creates the data repository. This is called when the provider attempts to open the
     * repository and SQLite reports that it doesn't exist.
     */
    @Override
    public void onCreate(SQLiteDatabase db) {

        // Creates the main table
        Recover = "FIRSTTIME";
        db.execSQL(SQL_CREATE_MAIN);
        Log.v("TABLE","CREATED");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " +  "MESSENGER");
        Log.v("TABLE", "DROPPED");
        onCreate(db);

    }

}