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
import static com.mongodb.client.model.Updates.*;

public class Update extends CRUD{
    public static void main (String [] args){
        updateFilePath( "/Users/hodamoharram/Desktop/bachelor\\?/Bottle.step","/Users/hodamoharram/Desktop/Bottle.step");


    }

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
        MongoCollection<Document> filesCollection = getFilesCollection();
        JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();
        Bson filter = eq("FilePath", filePath);
        Bson updateOperation = set(key, value);
            if (checkPathValidity(filePath) && key!="FilePath"){

                // update one document
                UpdateResult updateResult = filesCollection.updateOne(filter, updateOperation);

                // update the size, writes, and lastUpdate
                //save old size
                Document d = filesCollection.find(new Document("FilePath", filePath)).first();
                Double oldSize = (Double) d.get("FileSize in (KB)");


                UpdateResult updateSizeResult= filesCollection.updateOne(filter,updateSize(filePath));
                UpdateResult updateWrites = filesCollection.updateOne(filter, updateWrites(filePath));
                UpdateResult updateDate = filesCollection.updateOne(filter, updateDate(filePath));

                Document doc = filesCollection.find(new Document("FilePath", filePath)).first();
                updateStatistics(doc, oldSize);


                if (key == "FileName"){
                    String newFilePathCheck = updateFilePathByName(filePath, value);
                    Bson updateFilePath = set("FilePath", newFilePathCheck);

                    UpdateResult updateFilePathByName = filesCollection.updateOne(filter, updateFilePath);

                    if (!checkPathValidity(newFilePathCheck)){
                        Bson deletionFilter = eq(key, value);

                        Document document = filesCollection.find(new Document("FilePath", filePath)).first();
                        Double oldSize2 = (Double) document.get("FileSize in (KB)");


                        filesCollection.findOneAndDelete(deletionFilter);

                        Document document2 = filesCollection.find(new Document("FilePath", filePath)).first();
                        updateStatisticsWDelete(document2, oldSize2);
                        out.println("File path: " + filePath + " is invalid and removed from database.");
                    }
                }


                out.println("=> Updating the doc with {\"FilePath\":"+ filePath + "}. Updating" + key + value + ".");

            }
            else{
                if (key=="FilePath"){
                    UpdateResult updateResult = filesCollection.updateOne(filter, updateOperation);
                    String newFileName = getFileName(value.toString());
                    Bson updateFileNameByPath = set("FileName", newFileName);
                    UpdateResult updateResultName = filesCollection.updateOne(filter, updateFileNameByPath);
                    Bson filter2 = eq("FilePath", value.toString());

                    //save old size
                    Document d = filesCollection.find(new Document("FilePath", filePath)).first();
                    Double oldSize = (Double) d.get("FileSize in (KB)");

                    // update the size, writes, and lastUpdate
                    UpdateResult updateSizeResult= filesCollection.updateOne(filter2,updateSize(value.toString()));
                    UpdateResult updateWrites = filesCollection.updateOne(filter2, updateWrites(value.toString()));
                    UpdateResult updateDate = filesCollection.updateOne(filter2, updateDate(value.toString()));

                    //update stats
                    Document doc = filesCollection.find(new Document("FilePath", filePath)).first();
                    updateStatistics(doc, oldSize);
                }
                else{
                    out.println("File path: " + filePath + " is invalid. Update operation failed.");

                }

            }

        out.flush();

    }
    private static void updateStatistics(Document d, Double oldSize){

        String fp =(String) d.get("FilePath");
        File f = new File (fp);
        Double newFileSize = getFileSizeKiloBytes(f);
        Double diff = newFileSize - oldSize;

        MongoCollection<Document> stats = getStatsCollection();

        Bson filter1 = eq("Name","Statistics Document");
        Document statsDoc = stats.find(filter1).first();



        int filesCount = (int) statsDoc.get("Files Count") ;
        Double totalFileSizes = (Double) statsDoc.get("Total Files Storage Space in KB") + diff;
        int totalWritesCount = (int) statsDoc.get("CountTotalWrites") + 1;
        Double avgFileSize = totalFileSizes / filesCount;


        Bson update1 = set("Total Files Storage Space in KB", totalFileSizes);
        Bson update2 = set("CountTotalWrites", totalWritesCount);
        Bson update3 = set("Average File Size", avgFileSize);
        Bson updateOperations = combine( update1, update2, update3);
        stats.findOneAndUpdate(filter1, updateOperations);


    }

    private static void updateStatisticsWDelete(Document d, Double oldSize){


        MongoCollection<Document> stats = getStatsCollection();

        Bson filter1 = eq("Name","Statistics Document");
        Document statsDoc = stats.find(filter1).first();



        int filesCount = (int) statsDoc.get("Files Count");
        Double totalFileSizes = (Double) statsDoc.get("Total Files Storage Space in KB") - oldSize;
        int totalWritesCount = (int) statsDoc.get("CountTotalWrites") + 1;
        Double avgFileSize = totalFileSizes / filesCount;

        Bson update1 = set("Total Files Storage Space in KB", totalFileSizes);
        Bson update2 = set("CountTotalWrites", totalWritesCount);
        Bson update3 = set("Average File Size", avgFileSize);
        Bson update4 = set("Files Count", filesCount);

        Bson updateOperations = combine( update1, update2, update3, update4);
        stats.findOneAndUpdate(filter1, updateOperations);


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
    private static String updateFilePathByName(String filePath, Object value){
        int lastPeriod = filePath.lastIndexOf(".");
        String extension = "."+ filePath.substring(lastPeriod + 1);
        int lastSlash = filePath.lastIndexOf("/");
        String newFilePath = filePath.substring(0, lastSlash + 1) + value + extension + "";
        return newFilePath;
    }

}