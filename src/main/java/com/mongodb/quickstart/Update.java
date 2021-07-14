package com.mongodb.quickstart;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import java.io.File;
import java.util.Date;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;

public class Update {

    public static void updateFileName (String filePath, Object value){
        updateOneDocument(filePath, "FileName", value);
        //requires updating the filePath
    }
    public static void updateFilePath (String filePath, Object value){
        updateOneDocument(filePath, "FilePath", value);
    }
    public static void updateDescription (String filePath, Object value){
        updateOneDocument(filePath, "FileDescription", value);
    }
    public static void updateFileSize (String filePath, Object value){
        updateOneDocument(filePath, "FileSize in (KB)", value);
    }

    private static void updateOneDocument(String filePath, String key, Object value) {
        try (MongoClient mongoClient = MongoClients.create("mongodb+srv://m220student:m220password@cluster0.fehw5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> filesCollection = sampleTrainingDB.getCollection("Just work");

            JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();

            if (checkPathValidity(filePath)){

                // update one document
                Bson filter = eq("FilePath", filePath);
                Bson updateOperation = set(key, value);
                UpdateResult updateResult = filesCollection.updateOne(filter, updateOperation);

                // update the size, writes, and lastUpdate
                UpdateResult updateSizeResult= filesCollection.updateOne(filter,updateSize(filePath));
                UpdateResult updateWrites = filesCollection.updateOne(filter, updateWrites(filePath));
                UpdateResult updateDate = filesCollection.updateOne(filter, updateDate(filePath));



                System.out.println("=> Updating the doc with {\"FilePath\":"+ filePath + "}. Updating" + key + value + ".");
                System.out.println(filesCollection.find(filter).first().toJson(prettyPrint));
                System.out.println(updateResult);


            }
            else{
                System.out.println("File path is invalid. Update operation failed.");
            }
        }
    }



    private static Double getFileSizeKiloBytes(File file) {
        return (double) file.length() / 1024;
    }

    private static boolean checkPathValidity (String filePath){
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()){
            return false;
        }
        else{
            return true;
        }
    }
    private static Bson updateSize(String filePath){
        File file = new File(filePath);
        Double fileSize = getFileSizeKiloBytes(file);
        Bson updateSize = set("FileSize in (KB)", fileSize);
        return updateSize;
    }

    private static Bson updateWrites(String filePath){
        Bson filter = eq("FilePath", filePath);
        Bson updateOperation = inc("CountWrites", 1);
        return updateOperation;
    }
    private static Bson updateDate(String filePath){
        Date date = new Date();
        Bson updateLastUpdated = set("LastUpdated", date);
        return updateLastUpdated;
    }

}