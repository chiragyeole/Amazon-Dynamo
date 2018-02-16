package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.content.ContentResolver;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Messenger;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import android.app.Activity;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import java.net.Socket;
import java.net.ServerSocket;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.content.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.lang.*;

public class SimpleDynamoProvider extends ContentProvider {
    public static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    public static final String Provider_name = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final String Table_name = "MESSENGER";

    public final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    //public static final Uri CONTENT_URI = Uri.parse("content://" + Provider_name + "/MESSENGER");


    private MainDatabaseHelper mOpenHelper;

    private SQLiteDatabase db;

    private static final String DBNAME = "messengerDb";

    public static final String REMOTE_PORT0 = "11108";
    public static final String REMOTE_PORT1 = "11112";
    public static final String REMOTE_PORT2 = "11116";
    public static final String REMOTE_PORT3 = "11120";
    public static final String REMOTE_PORT4 = "11124";
    public static final Integer TIMEOUT = 1000;
    public static int NUMAVDS = 5;
    //public static ArrayList REMOTE_PORTS = new ArrayList<String>(Arrays.asList(REMOTE_PORT4,REMOTE_PORT1,REMOTE_PORT0,REMOTE_PORT2,REMOTE_PORT3));
    //public static ArrayList REMAINING_PORTS = new ArrayList<String>(Arrays.asList(REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4));
    public static int seqNumber = 0;
    public static int contentProviderseqNumber = -1;
    public static int counter = 0;
    public final int SERVER_PORT = 10000;
    public static long minKey = 0;
    public static int maxValue = -1;
    public static int maxAgreed = 0;
    public static int failedClient = -1;
    public static String recSelection;
    public static ArrayList<String> REMOTE_PORTS = new ArrayList<String>();
    public static ArrayList<String> REPLICATION_PORTS = new ArrayList<String>();
    public static ArrayList<String> QREPLICATION_PORTS = new ArrayList<String>();
    public static ArrayList<String> RECOVERY_PORTS = new ArrayList<String>();
    public static ArrayList<String> DELREPLICA_PORTS = new ArrayList<String>();
    public static ArrayList<String> allKeys = new ArrayList<String>();
    public static ArrayList<String> recoveryKeys = new ArrayList<String>();
    public static ArrayList<String> specQuery = new ArrayList<String>();
    public static ArrayList<String> delKeys = new ArrayList<String>();
    Hashtable<String,Integer> recoveryMap = new Hashtable<String, Integer>();
    public static ArrayList<String> keyFromReplicas = new ArrayList<String>();
    public static ArrayList<String> valFromReplicas = new ArrayList<String>();
    public static ArrayList<Integer> versionFromReplicas = new ArrayList<Integer>();
    private static final Object lock1 = new Object();
    private static final Object lock2 = new Object();
    @Override
    public boolean onCreate() {
        mOpenHelper = new MainDatabaseHelper(getContext());

        db = mOpenHelper.getWritableDatabase();

        Log.i("Database", "Database created");
        REMOTE_PORTS.add(REMOTE_PORT4);
        REMOTE_PORTS.add(REMOTE_PORT1);
        REMOTE_PORTS.add(REMOTE_PORT0);
        REMOTE_PORTS.add(REMOTE_PORT2);
        REMOTE_PORTS.add(REMOTE_PORT3);
        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        //		String myPort = getPort();
        //		String msg = "NODE JOIN";
        //		if (!myPort.equals("11108")) {
        //			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
        //		} else {
        //
        //		}

        return false;
    }

    public String getPort() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }

    public Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        int count = 0;

        try {
            if (!(REMOTE_PORTS.isEmpty()) && !(delKeys.contains(selection)) && !(selection.equals("@")) && !(selection.equals("*"))) {
                //DELREPLICA_PORTS.add(getPort());
                for (int i = 0; i < REMOTE_PORTS.size(); i++) {

                    int j = (Integer.parseInt(REMOTE_PORTS.get(i)) / 2);
                    int r = 0;
                    if (i == (REMOTE_PORTS.size() - 1)) {
                        r = (Integer.parseInt(REMOTE_PORTS.get(0)) / 2);

                    } else {
                        r = (Integer.parseInt(REMOTE_PORTS.get(i + 1)) / 2);
                    }
                    if (((genHash(selection).compareTo(genHash(Integer.toString(j))) > 0) && (genHash(selection).compareTo(genHash(Integer.toString(r))) <= 0)
                            && (genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) < 0))
                            || ((genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) > 0) && (genHash(selection).compareTo(genHash(Integer.toString(r))) <= 0))
                            || ((genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) > 0) && (genHash(selection).compareTo(genHash(Integer.toString(r))) > 0))) {
                        String deleteToPort = Integer.toString(r * 2);
                        DELREPLICA_PORTS.add(deleteToPort);
                        if (i == REMOTE_PORTS.size() - 1) {
                            DELREPLICA_PORTS.add(REMOTE_PORTS.get(1));
                            DELREPLICA_PORTS.add(REMOTE_PORTS.get(2));

                        } else if (i == REMOTE_PORTS.size() - 2) {
                            DELREPLICA_PORTS.add(REMOTE_PORTS.get(0));
                            DELREPLICA_PORTS.add(REMOTE_PORTS.get(1));
                        } else if (i == REMOTE_PORTS.size() - 3) {
                            DELREPLICA_PORTS.add(REMOTE_PORTS.get(4));
                            DELREPLICA_PORTS.add(REMOTE_PORTS.get(0));
                        } else {
                            DELREPLICA_PORTS.add(REMOTE_PORTS.get(i + 2));
                            DELREPLICA_PORTS.add(REMOTE_PORTS.get(i + 3));
                        }
                        break;
                    }
                }
                Log.i("Size of Deletion ", "SIZE " + DELREPLICA_PORTS.size() + " " + selection);
                for (int i = 0; i < DELREPLICA_PORTS.size(); i++) {
                    String msgQuery = selection;
                    Log.i("Sending to Delete ", selection + "to Port: " + DELREPLICA_PORTS.get(i));
                    String msgToSend = "DELETE KEY" + "," + msgQuery + "," + getPort();
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(DELREPLICA_PORTS.get(i)));
                    //String msgToSend = "INSERT KEY" + "," + msgInsert;
                    DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                    outstream.writeUTF(msgToSend);
                    outstream.close();
                    socket.close();
                }
                DELREPLICA_PORTS.clear();
                delKeys.clear();;
            }
            else if (selection.equals("*")) {
                count = db.delete(Table_name, "1", null);
                return count;
            } else if (selection.equals("@")) {
                count = db.delete(Table_name, "1", null);
                return count;
            } else {

                Log.i("DELETED",selection);
                count = db.delete(Table_name, "key=" + "'" + selection + "'", null);

                return count;

            }
        }
        catch (IOException e) {
            Log.e(TAG, "IO Exception");
        }
        catch(NoSuchAlgorithmException e)
        {
            Log.e(TAG,"NO Such Algorithm");
        }
        //e.printStackTrace();

        //Log.e(TAG,"Cursor key= "+ key1);
        //Log.e(TAG,"Cursor value= "+ value1);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public  Uri insert(Uri uri, ContentValues values) {
        String insertToPort = "";
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String portversion = values.getAsString("portversion");
        Log.e(TAG,"Inside Insert " + key +" "+value+" "+ portversion);
        //synchronized (lock1) {
        try {
            synchronized (this) {
                if (!(REMOTE_PORTS.isEmpty()) && !(allKeys.contains(key)) && !(recoveryKeys.contains(key))) {
                    //Log.e(TAG, "KEY TO INSERT: "+ key + " with hash value: "+ genHash(key));
                    for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                        //Log.i("CHECK KEY", ORDER_PORTS.get(i));
                        int j = (Integer.parseInt(REMOTE_PORTS.get(i)) / 2);
                        int r = 0;
                        if (i == (REMOTE_PORTS.size() - 1)) {
                            r = (Integer.parseInt(REMOTE_PORTS.get(0)) / 2);

                        } else {
                            r = (Integer.parseInt(REMOTE_PORTS.get(i + 1)) / 2);
                        }
                        if (((genHash(key).compareTo(genHash(Integer.toString(j))) > 0) && (genHash(key).compareTo(genHash(Integer.toString(r))) <= 0)
                                && (genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) < 0))
                                || ((genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) > 0) && (genHash(key).compareTo(genHash(Integer.toString(r))) <= 0))
                                || ((genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) > 0) && (genHash(key).compareTo(genHash(Integer.toString(r))) > 0))) {
                            insertToPort = Integer.toString(r * 2);
                            REPLICATION_PORTS.add(insertToPort);
                            if (i == REMOTE_PORTS.size() - 1) {
                                REPLICATION_PORTS.add(REMOTE_PORTS.get(1));
                                REPLICATION_PORTS.add(REMOTE_PORTS.get(2));

                            } else if (i == REMOTE_PORTS.size() - 2) {
                                REPLICATION_PORTS.add(REMOTE_PORTS.get(0));
                                REPLICATION_PORTS.add(REMOTE_PORTS.get(1));
                            } else if (i == REMOTE_PORTS.size() - 3) {
                                REPLICATION_PORTS.add(REMOTE_PORTS.get(4));
                                REPLICATION_PORTS.add(REMOTE_PORTS.get(0));
                            } else {
                                REPLICATION_PORTS.add(REMOTE_PORTS.get(i + 2));
                                REPLICATION_PORTS.add(REMOTE_PORTS.get(i + 3));
                            }
                            //Log.i("Port closest greater:", Integer.toString(r*2));
                            break;
                        }
                        insertToPort = "NOT PRESENT";
                    }

                    if (!(insertToPort.equals("NOT PRESENT"))) {
                        Log.i("Size of Replication ", "SIZE " + REPLICATION_PORTS.size() + " "+ key );
                        for (int i = 0; i < REPLICATION_PORTS.size(); i++) {

                            Log.i("Sending to Insert ", key + "to Port: " + REPLICATION_PORTS.get(i));
                            String msgInsert = key + "," + value+ "," + insertToPort;
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REPLICATION_PORTS.get(i)));
                            String msgToSend = "INSERT KEY" + "," + msgInsert;
                            DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                            outstream.writeUTF(msgToSend);
                            outstream.close();
                            socket.close();
                        }
                        REPLICATION_PORTS.clear();
                    }
                }
            }
        } catch (IOException e) {
            Log.e(TAG, "IO Exception");
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No Algorithm Exception");
            e.printStackTrace();
        }
        // }
        synchronized (this) {
            if (insertToPort.equals("")) {
                specQuery.add(key);
                Cursor c3 = query(mUri, null, key, null, null);
                specQuery.clear();
                Object[] data10 = new Object[3];
                Log.e(TAG, "COUNT: " + c3.getCount() + " " + portversion);

                if (!(c3.getCount() == 0)) {
                    if (c3.moveToFirst()) {
                        do {
                            data10[0] = c3.getString(c3.getColumnIndex("key"));
                            //Log.i("key in database", "key:" + data10[0]);
                            data10[1] = c3.getString(c3.getColumnIndex("value"));
                            Log.i("value in database", "value:" + data10[1]);
                            data10[2] = c3.getString(c3.getColumnIndex("portversion"));
                            Log.i("version in database", "version:" + data10[2]);
                            // do what ever you want here
                        } while (c3.moveToNext());
                    }
                    //int c = recoveryMap.get(key)+1;
                    String value2 = "" + data10[1];
                    values.put("value", value);
                    String version = "" + data10[2];
                    String version1[] = version.split("&");
                    String version2 = version1[0] + "&" + (Integer.parseInt(version1[1]) + 1);
                    values.put("portversion", (version2));
                    Log.i("update", "updating key: " + key + " with value " + value);
                    db.update(Table_name, values, "key=" + "'" + key + "'", null);
                } else {
                    values.put("key", key);
                    values.put("value", value);
                    Log.e(TAG,"PORTVERSION INSERT: "+ portversion);
                    values.put("portversion", portversion);
                    long id = db.insert(Table_name, null, values);

                    if (id > 0) {

                        Uri newuri = ContentUris.withAppendedId(uri, id);

                        Log.i("insert", "Inserting key: " + key + " with value " + value);
                        return newuri;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public  Cursor query(Uri uri, String[] projection, String selection,
                         String[] selectionArgs, String sortOrder) {
        //Log.i("Selection:" , selection);
        MatrixCursor mc = new MatrixCursor(new String[]{"key","value"});
        MatrixCursor mc1 = new MatrixCursor(new String[]{"key","value"});
        String queryToPort = "";
        mOpenHelper = new MainDatabaseHelper(getContext());
        //db = mOpenHelper.getWritableDatabase();
        String exceptionPort = "";
        // String myportToSend = null;
        String selectionToSend = "";
        SQLiteQueryBuilder b = new SQLiteQueryBuilder();
        b.setTables(Table_name);
        //db.query(Table_name, new String[]{"key", "value"}, "key=" + "'" + selection + "'", null, null, null, null);
        String specKey = "Select key,value,portversion from MESSENGER where key =" + "\"" + selection + "\"";
        String localKeys = "Select key,value,portversion from MESSENGER ";
        String atKeys = "Select key,value from MESSENGER ";
        String recoverQuery = "SELECT key,value,portversion from MESSENGER where portversion like" + "'" + recSelection + "%'";
        String recoverBackQuery = "SELECT key,value,portversion from MESSENGER where portversion like" + "'" + getPort() + "%'";
        Log.i("query", selection);


        try {

            if (!(REMOTE_PORTS.isEmpty()) && !(specQuery.contains(selection)) && !(selection.equals("@")) && !(selection.equals("*")) && !(selection.equals("#")) && !(selection.equals("$"))) {
                synchronized (lock1) {
                    for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                        //Log.i("CHECK KEY", selection);
                        int j = (Integer.parseInt(REMOTE_PORTS.get(i)) / 2);
                        int r = 0;
                        if (i == (REMOTE_PORTS.size() - 1)) {
                            r = (Integer.parseInt(REMOTE_PORTS.get(0)) / 2);

                        } else {
                            r = (Integer.parseInt(REMOTE_PORTS.get(i + 1)) / 2);
                        }
                        if (((genHash(selection).compareTo(genHash(Integer.toString(j))) > 0) && (genHash(selection).compareTo(genHash(Integer.toString(r))) <= 0)
                                && (genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) < 0))
                                || ((genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) > 0) && (genHash(selection).compareTo(genHash(Integer.toString(r))) <= 0))
                                || ((genHash(Integer.toString(j)).compareTo(genHash(Integer.toString(r))) > 0) && (genHash(selection).compareTo(genHash(Integer.toString(r))) > 0))) {


                            queryToPort = Integer.toString(r * 2);
                            Log.i("Port closest greater:", Integer.toString(r * 2));
                            QREPLICATION_PORTS.add(queryToPort);
                            if (i == REMOTE_PORTS.size() - 1) {
                                QREPLICATION_PORTS.add(REMOTE_PORTS.get(1));
                                QREPLICATION_PORTS.add(REMOTE_PORTS.get(2));

                            } else if (i == REMOTE_PORTS.size() - 2) {
                                QREPLICATION_PORTS.add(REMOTE_PORTS.get(0));
                                QREPLICATION_PORTS.add(REMOTE_PORTS.get(1));
                            } else if (i == REMOTE_PORTS.size() - 3) {
                                QREPLICATION_PORTS.add(REMOTE_PORTS.get(4));
                                QREPLICATION_PORTS.add(REMOTE_PORTS.get(0));
                            } else {
                                QREPLICATION_PORTS.add(REMOTE_PORTS.get(i + 2));
                                QREPLICATION_PORTS.add(REMOTE_PORTS.get(i + 3));
                            }
                            break;
                        }
                    //}
                }
                selectionToSend = selection;
                Log.e(TAG, "Size of QREplicaion: "+QREPLICATION_PORTS.size() + " " + selection);
                    for (int i = 0; i < QREPLICATION_PORTS.size(); i++) {
                        try{
                        String msgQuery = selection;

                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(QREPLICATION_PORTS.get(i)));
                        String msgToSend = "QUERY KEY" + "," + msgQuery + "," + getPort();
                        Log.i("Sending to Query: ", selection + " " + QREPLICATION_PORTS.get(i));
                        exceptionPort = QREPLICATION_PORTS.get(i);
                        DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                        DataInputStream is = new DataInputStream(socket.getInputStream());
                        outstream.writeUTF(msgToSend);

                        String returnedCursor = is.readUTF();
                        Log.e(TAG, "returnedCursor: " + returnedCursor);
                        Object o[] = returnedCursor.split(",");
                        Log.e(TAG, "AFTER SPLITTING: " + o[0] + " " + o[1] + " " + o[2]);
                        String version2 = "" + o[2];
                        String oldValue[] = version2.split("&");

                        Log.i("Value versions", "versionAQ: " + oldValue[1]);
                        Log.i("Key After Query", "keyAQ: " + o[0]);
                        Log.i("actual value", "valueAQ: " + o[1]);
                        String value3 = "" + o[1];
                        String key3 = "" + o[0];
                        keyFromReplicas.add(key3);
                        versionFromReplicas.add(Integer.parseInt(oldValue[1]));
                        valFromReplicas.add(value3);
                        //Object p[] = new Object[2];
                        //p[0] = o[0];
                        //p[1] = o[1];
                        //mc.addRow(p);
                        if (returnedCursor != null) {
                            is.close();
                            outstream.close();
                            socket.close();
                            //mc.close();
                        }
                    }
                        catch(IOException e)
                        {
                            Log.e(TAG, "IO Exception");
                        }
                    }

                    int position = versionFromReplicas.indexOf(Collections.max(versionFromReplicas));
                    Object p[] = new Object[2];
                    p[0] = keyFromReplicas.get(position);
                    p[1] = valFromReplicas.get(position);
                    mc.addRow(p);
                    keyFromReplicas.clear();
                    valFromReplicas.clear();
                    versionFromReplicas.clear();
                    QREPLICATION_PORTS.clear();
                    //specQuery.clear();
                    return mc;
                }
            }

            else
            if (!(REMOTE_PORTS.isEmpty()) && !(specQuery.contains(selection)) && (selection.equals("*")) && !(selection.equals("@")) && !(selection.equals("#")) && !(selection.equals("$")) ) {
                for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                    try {
                        String msgQuery = "*";
                        exceptionPort = REMOTE_PORTS.get(i);
                        selectionToSend = selection;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS.get(i)));
                        String msgToSend = "QUERY ALLKEYS" + "," + msgQuery + "," + getPort();
                        DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                        DataInputStream is = new DataInputStream(socket.getInputStream());
                        outstream.writeUTF(msgToSend);
                        String returnedCursor = is.readUTF();
                        //Log.e(TAG, "NO" + returnedCursor + "SPACE");
                        if ((returnedCursor != null) && (returnedCursor.trim().length() != 0)) {
                            // Log.e(TAG, "returnedCursor: " + returnedCursor);
                            String splitRows[] = returnedCursor.split(",");
                            for (int f = 0; f < splitRows.length; f++) {
                                //Log.e(TAG, "AFTER SPLITTING: " + splitRows[f]);
                                Object o[] = splitRows[f].split(":");
                                Object p[] = new Object[2];
                                p[0] = o[0];
                                p[1] = o[1];
                                mc1.addRow(p);
                            }
                        }
                        Log.e(TAG, "COUNT in CURSOR: " + mc1.getCount());
                        if (returnedCursor != null) {
                            is.close();
                            outstream.close();
                            socket.close();
                        }
                    }
                    catch(IOException e)
                    {
                        Log.e(TAG, "IO Exception");
                    }
                }
                Log.e(TAG, "COUNT in CURSOR: " + mc1.getCount());
                //specQuery.clear();
                mc1.close();
                return mc1;
            }
            //}

            if (selection.equals("*")) {
                Cursor c = mOpenHelper.getReadableDatabase().rawQuery(localKeys, null);
                //c.close();
                return c;
            } else if (selection.equals("@")) {
                synchronized (lock1) {
                    Cursor c = mOpenHelper.getReadableDatabase().rawQuery(atKeys, null);
                    //c.close();
                    return c;
                }
            } else if (selection.equals("#")) {
                Cursor c = mOpenHelper.getReadableDatabase().rawQuery(recoverQuery, null);
                return c;
            } else if (selection.equals("$")) {
                Cursor c = mOpenHelper.getReadableDatabase().rawQuery(recoverBackQuery, null);
                return c;
            } else {
                Cursor c = mOpenHelper.getReadableDatabase().rawQuery(specKey, null);
                Log.e(TAG, "GOT THE KEY");
                //c.close();
                return c;
            }


        }

        catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No Algorithm Exception");
            // e.printStackTrace();
        }
        //Log.e(TAG,"Cursor key= "+ key1);
        //Log.e(TAG,"Cursor value= "+ value1);
        return null;
    }


    public Cursor queryAgain (String exceptionPort,String selectionToSend)
    {
        try {

            MatrixCursor mc2 = new MatrixCursor(new String[]{"key", "value"});
            MatrixCursor mc3 = new MatrixCursor(new String[]{"key", "value"});
            Log.i("Sending to QueryAgain: ", selectionToSend);

            String queryToPort1="";
            if(!(selectionToSend.equals("*"))) {
                String msgQuery = selectionToSend;
                for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                    if (exceptionPort.contains(REMOTE_PORTS.get(i))) {
                        if (i == REMOTE_PORTS.size() - 1) {
                            queryToPort1 = REMOTE_PORTS.get(0);


                        } else if (i == REMOTE_PORTS.size() - 2) {
                            queryToPort1 = REMOTE_PORTS.get(4);

                        } else if (i == REMOTE_PORTS.size() - 3) {
                            queryToPort1 = REMOTE_PORTS.get(3);

                        } else {
                            queryToPort1 = REMOTE_PORTS.get(i + 1);

                        }
                    }
                }

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(queryToPort1));
                String msgToSend = "QUERY KEY" + "," + msgQuery + "," + getPort();
                DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                DataInputStream is = new DataInputStream(socket.getInputStream());
                outstream.writeUTF(msgToSend);

                String returnedCursor = is.readUTF();
                //Log.e(TAG,"returnedCursor: "+ returnedCursor);
                Object o[] = returnedCursor.split(",");
                Object p[] = new Object[2];
                p[0] = o[0];
                p[1] = o[1];
                mc2.addRow(p);
                if (returnedCursor != null) {
                    is.close();
                    outstream.close();
                    socket.close();
                    mc2.close();
                }
                return mc2;
            }
            else
            {
                for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                    Log.i("Query * Again", REMOTE_PORTS.get(i) + " ExPort " + exceptionPort);
                    if (!(REMOTE_PORTS.get(i).contains(exceptionPort))) {
                        String msgQuery = "*";
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS.get(i)));
                        String msgToSend = "QUERY ALLKEYS" + "," + msgQuery + "," + getPort();
                        DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                        DataInputStream is = new DataInputStream(socket.getInputStream());
                        outstream.writeUTF(msgToSend);
                        String returnedCursor = is.readUTF();
                        //Log.e(TAG, "NO" + returnedCursor + "SPACE");
                        if ((returnedCursor != null) && (returnedCursor.trim().length() != 0)) {
                            // Log.e(TAG, "returnedCursor: " + returnedCursor);
                            String splitRows[] = returnedCursor.split(",");
                            for (int f = 0; f < splitRows.length; f++) {
                                // Log.e(TAG, "AFTER SPLITTING: " + splitRows[f]);
                                Object o[] = splitRows[f].split(":");
                                Object p[] = new Object[2];
                                p[0] = o[0];
                                p[1] = o[1];
                                mc3.addRow(p);
                            }
                        }
                        Log.e(TAG, "COUNT in CURSOR inside For loop: " + mc3.getCount());
                        if (returnedCursor != null) {
                            is.close();
                            outstream.close();
                            socket.close();
                        }


                    }
                }
                Log.e(TAG, "COUNT in CURSOR: " + mc3.getCount());
                mc3.close();
                return mc3;

            }
        }
        catch(IOException e)
        {
            Log.e(TAG,"IO Exception");
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    //////// Server Task Starting ///////////
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            if(MainDatabaseHelper.Recover.contains("RECOVER")){
                {
                    synchronized (lock1){
                        RecoverData();}
                }
                //                HashMap<String, String> recoveredMap = reoveryTaskTemp();
                //                recoverData(recoveredMap);
            }
            Log.i("Server Task","INSIDE");
            try{
                while(true) {
                    Socket clientSocket = serverSocket.accept();
                    //Log.e(TAG, "Inside try");

                    DataInputStream is = new DataInputStream(clientSocket.getInputStream());
                    //DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                    String data = is.readUTF();
                    Log.i("Message", data);
                    String receivedMsg[] = data.split(",");
                    // receivedMsg[0] = ack  receivedMsg[1] = Requestor Port

                    if (receivedMsg[0].contains("INSERT KEY")) {
                        int c = 1;
                        //String valuewithversion = receivedMsg[2]+"&"+ c;
                        allKeys.add(receivedMsg[1]);
                        recoveryMap.put(receivedMsg[1],c);
                        Log.i("NON DUPLICATE" ,   receivedMsg[1]+ " "+ receivedMsg[2]);
                        ContentValues values1 = new ContentValues();
                        values1.put("key", (receivedMsg[1]));
                        values1.put("value", (receivedMsg[2]));
                        values1.put("portversion", (receivedMsg[3]+"&"+ c));
                        Uri uri = insert(mUri, values1);
                        allKeys.clear();
                    }
                    else if (receivedMsg[0].contains("QUERY KEY")) {
                        Cursor c1;
                        Log.i("QUERY KEY:", "PORT: " + getPort());
                        synchronized (this) {
                            specQuery.add(receivedMsg[1]);
                            c1 = query(mUri, null, receivedMsg[1], null, null);
                            specQuery.clear();
                        }
                        Object[] data1 = new Object[3];
                        if (c1.moveToFirst()) {
                            do {
                                data1[0] = c1.getString(c1.getColumnIndex("key"));
                                data1[1] = c1.getString(c1.getColumnIndex("value"));
                                data1[2] = c1.getString(c1.getColumnIndex("portversion"));
                                // do what ever you want here
                            } while (c1.moveToNext());
                        }

                        //c1.close();
                        String data2 = data1[0] + "," + data1[1] + "," + data1[2];
                        Log.e(TAG, "CURSOR STRING: " + data2);
                        //Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt());
                        DataOutputStream outstream = new DataOutputStream(clientSocket.getOutputStream());
                        outstream.writeUTF(data2);
                        outstream.close();

                        //clientSocket.close();

                    }
                    else if (receivedMsg[0].contains("QUERY ALLKEYS"))
                    {   String data4="";
                        Log.i("QUERY ALLKEYS:" , "PORT: "+ getPort());
                        specQuery.add(receivedMsg[1]);
                        Cursor d = query(mUri, null,receivedMsg[1], null, null);
                        specQuery.clear();
                        Object[] data3=new Object[3];
                        if (d.moveToFirst()){
                            do {
                                data3[0] = d.getString(d.getColumnIndex("key"));
                                data3[1] = d.getString(d.getColumnIndex("value"));
                                data3[2]= d.getString(d.getColumnIndex("portversion"));
                                data4 = data4 + data3[0] + ":"+ data3[1] + ":" + data3[2] + ",";
                            }  while (d.moveToNext());
                        }
                        Log.e(TAG,"CURSOR STRING: " + data4);
                        //Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt());
                        if (!(data4.equals(null))&& !(data4.equals(" "))) {
                            DataOutputStream outstream = new DataOutputStream(clientSocket.getOutputStream());
                            outstream.writeUTF(data4);
                            outstream.close();
                        }
                    }
                    else if (receivedMsg[0].contains("RECOVER"))
                    {
                        Log.i("SERVER TASK","Inside Recovery method "+receivedMsg[2]);
                        recSelection = receivedMsg[2];
                        Cursor p = query(mUri,null, receivedMsg[1] ,null,null);
                        String data9 = "";
                        Object[] data8=new Object[3];
                        if (p.moveToFirst()){
                            do {
                                data8[0] = p.getString(p.getColumnIndex("key"));
                                data8[1] = p.getString(p.getColumnIndex("value"));
                                data8[2] = p.getString(p.getColumnIndex("portversion"));
                                data9 = data9 + data8[0] + ":"+ data8[1] + ":"+data8[2]+",";

                            }  while (p.moveToNext());
                        }
                        //.e(TAG,"RECOVERED STRING: " + data9);
                        //Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt());
                        if (!(data9.equals(null))&& !(data9.equals(" "))) {
                            DataOutputStream outstream = new DataOutputStream(clientSocket.getOutputStream());
                            outstream.writeUTF(data9);
                            outstream.close();
                        }

                    }
                    else if (receivedMsg[0].contains("DELETE KEY"))
                    {
                        int Count =0;
                        delKeys.add(receivedMsg[1]);
                        Count = delete(mUri, receivedMsg[1],null);

                    }

                    //                    seqNumber = seqNumber +1 ;
                    //                    ContentValues values = new ContentValues();
                    //                    values.put("key", (Integer.toString(seqNumber)));
                    //                    values.put("value",data);
                    //                    Uri uri = insert(mUri,values);
                    //                    publishProgress(data);
                }
            }
            catch(IOException e)
            {
                Log.e(TAG, "Failed to accept connection");
                e.printStackTrace();
            }
            //            catch(NoSuchAlgorithmException e)
            //            {
            //                Log.e(TAG, "No such algorithm exception");
            //                e.printStackTrace();
            //            }


            return null;
        }

        protected void onProgressUpdate(String...strings) {

            return;
        }
    }
    ///////// To Recover Data ///////////
    private void RecoverData ()
    {

        try {
            Log.i("RECOVERY", "Start");
            String selection = "#";

            ////////////////  For storing values from next nodes ////////////
            for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                if (getPort().contains(REMOTE_PORTS.get(i))) {
                    if (i == REMOTE_PORTS.size() - 1) {
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(0));
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(1));

                    } else if (i == REMOTE_PORTS.size() - 2) {
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(4));
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(0));
                    } else if (i == REMOTE_PORTS.size() - 3) {
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(3));
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(4));
                    } else {
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(i + 1));
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(i + 2));
                    }
                }
            }
            Log.i("RECOVERY SIZE", "waiting " + RECOVERY_PORTS.size() + " " + getPort());
            for (int k = 0; k < RECOVERY_PORTS.size(); k++) {
                String msgQuery = "#";
                //String oldValue="";
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(RECOVERY_PORTS.get(k)));
                Log.i("Asking Port",RECOVERY_PORTS.get(k));
                String msgToSend = "RECOVER" + "," + msgQuery + "," + getPort();
                Log.i("RECOVERY MESSAGE",msgToSend);
                DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                DataInputStream is = new DataInputStream(socket.getInputStream());
                outstream.writeUTF(msgToSend);
                String returnedCursor = is.readUTF();
                Log.e(TAG, "NO" + returnedCursor + "SPACE");
                if ((returnedCursor != null) && (returnedCursor.trim().length() != 0)) {
                    Log.e(TAG, "returnedCursor: " + returnedCursor);
                    String splitRows[] = returnedCursor.split(",");
                    for (int f = 0; f < splitRows.length; f++) {
                        Log.e(TAG, "AFTER SPLITTING: " + splitRows[f]);
                        String o[] = splitRows[f].split(":");
                        ///recoveryMap.put(o[0],o[1]);
                        specQuery.add(o[0]);
                        Cursor c2 = query(mUri, null,o[0], null, null);
                        specQuery.clear();
                        Object[] data10=new Object[3];

                        if(c2.getCount() == 0) {
                            for(int i = 0 ; i < allKeys.size(); i++)
                                Log.e(TAG,"VALUE ALL KEY " + allKeys.get(i));
                            Log.i("Inserting in recovery" , o[0]+ " " + o[1]+ " " +o[2]);
                            String recoveredValue[] = o[2].split("&");
                            recoveryMap.put(o[0],Integer.parseInt(recoveredValue[1]));
                            allKeys.add(o[0]);
                            recoveryKeys.add(o[0]);
                            ContentValues values1 = new ContentValues();
                            values1.put("key", (o[0]));
                            values1.put("value", (o[1]));
                            values1.put("portversion", (o[2]));
                            Uri uri = insert(mUri, values1);
                        }
                        else
                        {
                            Log.i("Inside recovery map " , c2.getCount()+ " " +o[2]);
                            if (c2.moveToFirst()){
                                do{
                                    data10[0] = c2.getString(c2.getColumnIndex("key"));
                                    Log.i("key in database", "key:"+ data10[0]);
                                    data10[1]= c2.getString(c2.getColumnIndex("value"));
                                    Log.i("value in database", "value:"+ data10[1]);
                                    data10[2]= c2.getString(c2.getColumnIndex("portversion"));
                                    Log.i("version in database", "version:"+ data10[2]);
                                    // do what ever you want here
                                }while(c2.moveToNext());
                            }

                            String version1 = ""+data10[2];
                            String oldValue[] = version1.split("&");
                            Log.i("Old Value", "version: "+oldValue[1]);
                            String recoveredValue[] = o[2].split("&");
                            Log.i("Recovered Value",o[1] + " " + recoveredValue[1]);
                            if (Integer.parseInt(recoveredValue[1]) >=  Integer.parseInt(oldValue[1]))

                            {
                                Log.i("Duplicate recovery", o[0] + " " + o[1]);
                                //String where = "key" + "='" + o[0] + "'";
                                recoveryMap.put(o[0],Integer.parseInt(recoveredValue[1]));
                                ContentValues values1 = new ContentValues();
                                values1.put("value", o[1]);
                                values1.put("portversion", o[2]);
                                db.update(Table_name, values1, "key=" + "'" + o[0] + "'", null);
                            }

                        }
                    }
                }
                // Log.e(TAG, "COUNT in CURSOR: " + mc1.getCount());
                if(returnedCursor != null)
                {
                    is.close();
                    outstream.close();
                    socket.close();}
            }
            RECOVERY_PORTS.clear();
            recoveryKeys.clear();


            ////////////////  For storing values from previous nodes ////////////
            for (int i = 0; i < REMOTE_PORTS.size(); i++) {
                if (getPort().contains(REMOTE_PORTS.get(i))) {
                    if (i == REMOTE_PORTS.size() - 4) {
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(0));
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(4));

                    } else if (i == REMOTE_PORTS.size() - 5) {
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(3));
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(4));
                    } else {
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(i - 1));
                        RECOVERY_PORTS.add(REMOTE_PORTS.get(i - 2));
                    }
                }
            }
            Log.i("RECOVERY SIZE", "waiting " + RECOVERY_PORTS.size() + " " + getPort());
            for (int k = 0; k < RECOVERY_PORTS.size(); k++) {
                String msgQuery = "$";
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(RECOVERY_PORTS.get(k)));
                Log.i("Asking Port",RECOVERY_PORTS.get(k));
                String msgToSend = "RECOVER" + "," + msgQuery + "," + getPort();
                Log.i("RECOVERY MESSAGE",msgToSend);
                DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                DataInputStream is = new DataInputStream(socket.getInputStream());
                outstream.writeUTF(msgToSend);
                String returnedCursor = is.readUTF();
                // Log.e(TAG, "NO" + returnedCursor + "SPACE");
                if ((returnedCursor != null) && (returnedCursor.trim().length() != 0)) {
                    Log.e(TAG, "returnedCursor: " + returnedCursor);
                    String splitRows[] = returnedCursor.split(",");
                    for (int f = 0; f < splitRows.length; f++) {
                        Log.e(TAG, "AFTER SPLITTING: " + splitRows[f]);
                        String o[] = splitRows[f].split(":");
                        specQuery.add(o[0]);
                        Cursor c2 = query(mUri, null,o[0], null, null);
                        specQuery.clear();
                        Object[] data10=new Object[3];

                        //if(c2!=null)   if (!(allKeys.contains(o[0])))
                        ///recoveryMap.put(o[0],o[1]);
                        if (c2.getCount() == 0) {
                            for(int i = 0 ; i < allKeys.size(); i++)
                                Log.e(TAG,"VALUE ALL KEY " + allKeys.get(i));
                            allKeys.add(o[0]);
                            Log.i("Inserting in recovery" , o[0]+ " " + o[1]+ " " +o[2]);
                            String recoveredValue[] = o[2].split("&");
                            recoveryMap.put(o[0],Integer.parseInt(recoveredValue[1]));
                            recoveryKeys.add(o[0]);
                            ContentValues values1 = new ContentValues();
                            values1.put("key", (o[0]));
                            values1.put("value", (o[1]));
                            values1.put("portversion", (o[2]));
                            Uri uri = insert(mUri, values1);
                        } else {
                            Log.i("Inside recovery map " , c2.getCount() + " " +o[2]);
                            if (c2.moveToFirst()){
                                do{
                                    data10[0] = c2.getString(c2.getColumnIndex("key"));
                                    Log.i("key in database", "key:"+ data10[0]);
                                    data10[1]= c2.getString(c2.getColumnIndex("value"));
                                    Log.i("value in database", "value:"+ data10[1]);
                                    data10[2]= c2.getString(c2.getColumnIndex("portversion"));
                                    Log.i("version in database", "version:"+ data10[2]);
                                    // do what ever you want here
                                }while(c2.moveToNext());
                            }
                            String version1 = ""+data10[2];
                            String oldValue[] = version1.split("&");

                            Log.i("Old Value", "version: "+oldValue[1]);
                            String recoveredValue[] = o[2].split("&");
                            Log.i("Recovered Value",o[1] + " " + recoveredValue[1]);
                            if (Integer.parseInt(recoveredValue[1]) >= Integer.parseInt(oldValue[1]))
                            {
                                Log.i("Duplicate recovery", o[0] + " " + o[1]);
                                //String where = "key" + "='" + o[0] + "'";
                                recoveryMap.put(o[0],Integer.parseInt(recoveredValue[1]));
                                ContentValues values1 = new ContentValues();
                                values1.put("value", o[1]);
                                values1.put("portversion", o[2]);
                                db.update(Table_name, values1, "key=" + "'" + o[0] + "'", null);
                            }

                        }
                    }

                }
                // Log.e(TAG, "COUNT in CURSOR: " + mc1.getCount());
                if(returnedCursor != null) {
                    is.close();
                    outstream.close();
                    socket.close();
                }
            }
            RECOVERY_PORTS.clear();
            recoveryKeys.clear();

        }
        catch (IOException e) {
            Log.e(TAG, "IO Exception");
            //e.printStackTrace();
        }

    }
    /////// Client Task Starting ///////////

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String remotePort = REMOTE_PORT0;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                String msgToSend = msgs[0] + ","+ msgs[1];
                DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
                outstream.writeUTF(msgToSend);
                Log.i("Client Task","INSIDE");

                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                outstream.close();
                socket.close();


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
}
