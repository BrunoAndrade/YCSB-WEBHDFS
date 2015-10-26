package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.io.*;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Class for the measure of the HDFS system raw performance
 */
public class HdfsClient extends DB {

    public static final int Ok = 0;
    public static final int Error = -1;

    public static final String BASE_FOLDER_PROPERTY = "filesystem.base_folder";

    private WebHDFS conn;

    /**The base folder for the base system*/
    private String filesystem_base_folder;

    public void init() throws DBException {
        conn = new WebHDFS("hadoop-NN1", 50070, "hadoop", new Boolean(false));
        
        filesystem_base_folder = getProperties().getProperty(BASE_FOLDER_PROPERTY);
        if (filesystem_base_folder == null) {
            throw new DBException("Required property \"filesystem.base_folder\" missing for FileSystem client");
        }

    }
    
    public void cleanup() throws DBException {
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        //System.out.println(">Read Called> table: "+table+" key: "+key);

        String dest =filesystem_base_folder+"/"+key;
        boolean r = conn.open(dest);
        
        if(r) 
            return Ok;
        else
            return Error;
    }

    //private static boolean list(File dir, boolean recur, boolean size, boolean hidden) {
    private boolean list(String path, boolean recur) {
        JSONArray filelist = conn.listStatus(path);

        if(filelist.get(0).equals(new Integer(404))){
            System.err.println("Error:" + path + " not found!");
            return false;
        }
        else if(filelist == null){
            System.err.println("Error:" + path + "! Unknown error!");
            return false;
        }
        else if(filelist.isEmpty()){
            return true;
        }
        else {
            JSONObject aux = null;
            String type = null;
            String pathsuffix = null;

            for(int i = 0; i<filelist.size(); i++){
                aux = (JSONObject) filelist.get(i);
                type = (String) aux.get("type");

                if(type == "DIRECTORY" && recur){
                    if(!this.list(path + "/" + pathsuffix, recur))
                        return false;
                }
                
                if(type == "FILE"){
                    conn.open(new String(path + "/" + pathsuffix));
                }
            }

            return true;
        }
    }

    // Este scan percorre todo o filesystem, desde a base_folder
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return list(filesystem_base_folder, true) ? Ok : Error;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
            if(insert(table,key,values) == Ok)
                return Ok;
            else
                return Error;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        System.out.println(">Insert Called> table: "+table+" key: "+key);

        String dest =filesystem_base_folder+"/"+key;
        ByteArrayOutputStream b = null;

        try{        
            b = new ByteArrayOutputStream();
            ObjectOutputStream o = new ObjectOutputStream(b);
            o.writeObject(values);
        }            
        catch(IOException e){
            e.printStackTrace();
        }
        if(conn.createCopyFromLocal(dest, b.toByteArray(), "true"))
            return Ok;
        else
            return Error;
    }

    @Override
    public int delete(String table, String key) {
        System.out.println(">Delete Called>  key: " + key);

        String dest =filesystem_base_folder+"/"+key;

        // recursivo se a key for path.
        boolean result = conn.delete(dest, "true");

        if(result)
            return Ok;
        else
            return Error;
    }
}
