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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;
import static com.mongodb.quickstart.Delete.deleteRecord;

public class Update extends CRUD{

    /*
    The Update class updates a document by its file path. A user can update a file name, file path, file description, or file size.
    Any update of any condition would automatically check the validity of the file path, and further update the size, the date the document
    was last updated, and the number of writes; in order to maintain consistency.
     */

    public static void main (String [] args){
        updateFilePath( "/Users/hodamoharram/Desktop/Bottle.step", "/Users/hodamoharram/Desktop/bachelor\\?/Bottle.step");

    }

    /*
        Given the file path, this method updates the file name of a document
     */
    public static void updateFileName (String filePath, Object value){
        updateOneDocument(filePath, "FileName", value);
    }
    /*
        Given the file path, this method updates the file path of a document
     */
    public static void updateFilePath (String filePath, Object value){
        updateOneDocument(filePath, "FilePath", value);
    }
    /*
        Given the file path, this method updates the file description of a document
     */
    public static void updateDescription (String filePath, Object value){
        updateOneDocument(filePath, "FileDescription", value);
    }
    /*
        Given the file path, this method updates the file size of a document
     */
    public static void updateFileSize (String filePath, Object value){
        updateOneDocument(filePath, "FileSize in (KB)", value);
    }

    /*
    This method updates a document that satisfy a given condition, updating statistics and also checking file validity and file size
     */
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
                Bson filter3 = eq("FilePath", filePath);
                Document d = filesCollection.find(filter3).first();
                Double oldSize = (Double) d.get("FileSize in (KB)");


                UpdateResult updateSizeResult= filesCollection.updateOne(filter,updateSize(filePath));
                UpdateResult updateWrites = filesCollection.updateOne(filter, updateWrites(filePath));
                UpdateResult updateDate = filesCollection.updateOne(filter, updateDate(filePath));

                Bson filter4 = eq("FilePath", filePath);
                Document doc = filesCollection.find(filter4).first();
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
                        out.println("File path: " + filePath + " is invalid and removed from database."+ new Date());
                    }
                }
                out.println("=> Updating the doc with {\"FilePath\":"+ filePath + "}. Updating" + key + value + "." + new Date());
            }
            else{
                if (key=="FilePath"){
                    //save old size
                    Bson filter3 = eq("FilePath", filePath);
                    Document d = filesCollection.find(filter3).first();
                    Double oldSize = (Double) d.get("FileSize in (KB)");

                    UpdateResult updateResult = filesCollection.updateOne(filter, updateOperation);
                    String newFileName = getFileName(value.toString());
                    Bson updateFileNameByPath = set("FileName", newFileName);
                    UpdateResult updateResultName = filesCollection.updateOne(filter, updateFileNameByPath);
                    Bson filter2 = eq("FilePath", value.toString());

                    // update the size, writes, and lastUpdate
                    UpdateResult updateSizeResult= filesCollection.updateOne(filter2,updateSize(value.toString()));
                    UpdateResult updateWrites = filesCollection.updateOne(filter2, updateWrites(value.toString()));
                    UpdateResult updateDate = filesCollection.updateOne(filter2, updateDate(value.toString()));

                    //update stats
                    Document doc = filesCollection.find(new Document("FilePath", value)).first();
                    updateStatistics(doc, oldSize);
                }
                else{
                    out.println("File path: " + filePath + " is invalid. Update operation failed." + new Date());
                }
            }
        out.flush();
    }
    /*
     * Moves file on the system from the initialFilePath to the new FilePath and updates the database
     * Only succeeds if the initial filePath was stored in the database and is still valid
     * Also, the new FilePath needs to be valid
     */
    public static void moveFile(String initialFilePath, String newFilePath){
        Date timeStamp = new Date();
        MongoCollection<Document> filesCollection = getFilesCollection();
        Bson filter = eq("FilePath", initialFilePath);

        // If the filePath is not found in the database the operation fails
        if(filesCollection.countDocuments(filter) == 0){
            out.println(initialFilePath + " could not be moved as the file path does not exist in the database, on" + timeStamp);
            out.flush();
            return;
        }

        // If filePath is invalid the move operation fail. If the invalid file is in the database it is deleted
        if(!checkPathValidity(initialFilePath)){
            out.println(initialFilePath + " could not be moved as the file path does not exist in the file system, on " + timeStamp);
            deleteRecord(initialFilePath);
            out.flush();
            return;
        }

        //Otherwise move the file
        try {
            Path temp = Files.move(Paths.get(initialFilePath), Paths.get(newFilePath));
            if (temp != null){
                out.println(initialFilePath + " was moved to " + newFilePath + " on " + timeStamp);
                updateFilePath(initialFilePath, newFilePath);
            }else{
                out.println(initialFilePath + " was not moved to " + newFilePath + " on " + timeStamp);
            }
        } catch (IOException e) {
            out.println(initialFilePath + " was not moved to " + newFilePath + " as the new directory does not exist in the file system on " + timeStamp);
        }
        out.flush();
    }
    private static void updateStatistics(Document d, Double oldSize){

        String fp =(String) d.get("FilePath");
        File f = new File (fp);
        Double newFileSize = getFileSizeKiloBytes(f);
        Double diff = newFileSize - oldSize;
        Date lastUpdated = new Date();


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
        Bson update4 = set("FilesCollection lastUpdated", lastUpdated);

        Bson updateOperations = combine( update1, update2, update3, update4);
        stats.findOneAndUpdate(filter1, updateOperations);


    }

    private static void updateStatisticsWDelete(Document d, Double oldSize){


        MongoCollection<Document> stats = getStatsCollection();

        Bson filter1 = eq("Name","Statistics Document");
        Document statsDoc = stats.find(filter1).first();


        Date lastUpdated = new Date();
        int filesCount = (int) statsDoc.get("Files Count");
        Double totalFileSizes = (Double) statsDoc.get("Total Files Storage Space in KB") - oldSize;
        int totalWritesCount = (int) statsDoc.get("CountTotalWrites") + 1;
        Double avgFileSize = totalFileSizes / filesCount;

        Bson update1 = set("Total Files Storage Space in KB", totalFileSizes);
        Bson update2 = set("CountTotalWrites", totalWritesCount);
        Bson update3 = set("Average File Size", avgFileSize);
        Bson update4 = set("Files Count", filesCount);
        Bson update5 = set("FilesCollection lastUpdated", lastUpdated);

        Bson updateOperations = combine( update1, update2, update3, update4, update5);
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