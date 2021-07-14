package com.mongodb.quickstart;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.*;
import java.util.*;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/*
This class is concerned with all create operations in a
 */
public class Create extends CRUD{

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> fileCollections = sampleTrainingDB.getCollection("ARObjectsDatabase");
            String filePath = "C:/Users/Zeina Kandil/Downloads/ergonomic-bottle-1.snapshot.2/Bottle.obj";

            String filePath2 = "C:/Users/Zeina Kandil/Downloads/ergonomic-bottle-1.snapshot.2/Bottle.step";
            ArrayList<String > filePaths = new ArrayList<>(); filePaths.add(filePath); filePaths.add(filePath2); filePaths.add(filePath2);
            insertManyDocuments(filePaths);
            insertOneDocument("C:/Users/Zeina Kandil/Downloads/ergonomighjhgfghc-bottle-1.snapshot.2/Bottle.stls");
            insertOneDocument("C:/Users/Zeina Kandil/Downloads/ergonomic-bottle-1.snapshot.2/Bottle.stl");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* Takes as input a FilePath and inserts only valid FilePaths
    into the database with no duplicates
    */
    private static void insertOneDocument(String filePath){
        insertOneDocument(filePath, "Default file description");
    }
    private static void insertOneDocument(String filePath, String fileDescription){
        MongoCollection<Document> filesCollection = getFilesCollection();
        if(filesCollection.countDocuments(eq("FilePath", filePath)) == 0){
            Document document = saveNewFile(filePath, fileDescription);
            if(document == null){
                out.println(filePath + " is an invalid file Path and was not added to the database on " + new Date());
                out.flush();
                return;
            }
            updateStatistics(document);
            filesCollection.insertOne(document);
            out.println(filePath + " has been added to the database successfully on " + new Date());
        }
        else
            out.println(filePath + " was not added to the database as there is another file with the same file path on " + new Date());
        out.flush();
    }

    /* Takes as input an Arraylist of FilePaths and inserts only valid FilePaths
    into the database with no duplicates
    */
    private static void insertManyDocuments(ArrayList<String> filePaths){
        HashSet<String> hsFilePaths = getValidFilePaths(filePaths);
        if(hsFilePaths.isEmpty())
            return;
        MongoCollection<Document> filesCollection = getFilesCollection();
        List<Document> files = new ArrayList<>();
        for (String filePath: hsFilePaths) {
            Document document = saveNewFile(filePath, "Default file description");
            if(document == null){
                out.println(filePath + " is an invalid file Path and was not added to the database on " + new Date());
                continue;
            }
            updateStatistics(document);
            files.add(document);
        }
        filesCollection.insertMany(files, new InsertManyOptions().ordered(false));
        out.flush();
    }

    /* Takes as input an Arraylist of FilePaths and returns a
    hashset of no duplicate files and no invalid filePaths
    */
    public static HashSet<String> getValidFilePaths(ArrayList<String> filePaths){
        File file;
        String filePath;
        HashSet<String> hsFilePaths = new HashSet<>();
        MongoCollection<Document> filesCollection = getFilesCollection();

        for (int i = 0; i <filePaths.size() ; i++) {
            filePath = filePaths.get(i);
            file = new File(filePath);
            if (!file.exists() || !file.isFile()){
                out.println(filePath + " is an invalid file path and was not added to the database on " + new Date());
                continue;
            }
            if(filesCollection.countDocuments(eq("FilePath", filePath)) == 0){
                if(hsFilePaths.contains(filePath)){
                    out.println(filePath + " was not added to the database as there is another file with the same file path in the same insertMany operation on " + new Date());
                }else{
                    hsFilePaths.add(filePath);
                    out.println(filePath + " has been added to the database successfully on " + new Date());
                }
            }else{
                out.println(filePath + " was not added to the database as there is another file with the same file path on " + new Date());
            }
        }
        out.flush();
        return hsFilePaths;
    }


    private static Document saveNewFile(String filePath, String FileDescription){
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()){
            return null;
        }
        String fileName = getFileName(filePath);
        String fileDescription = FileDescription;
        Double fileSize = getFileSizeKiloBytes(file);
        Date date = new Date();

        Document fileDoc = new Document("FilePath", filePath)
                .append("FileName",fileName)
                .append("FileSize in (KB)", fileSize)
                .append("FileDescription",fileDescription)
                .append("LastUpdated", date)
                .append("CreatedOn", date)
                .append("CountWrites", (Integer) 1)
                .append("CountReads", (Integer) 0);
        return fileDoc;
    }

    private static void updateStatistics(Document fileDocument){
        //Retrieve collections
        MongoCollection<Document> statsCollection = getStatsCollection();
        MongoCollection<Document> filesCollection = getFilesCollection();

        // Extract relevant attributes from fileDocument
        Double fileSize = (Double) fileDocument.get("FileSize in (KB)");
        Date lastUpdated = new Date();

        /*
        As create is the first operation that can be executed from the CRUD operations at the very first create call
        there will be no stats. In this case we need to create a stats Document
        */
        if(statsCollection.countDocuments() == 0){
            Document statsDoc = new Document("Name", "Statistics Document")
                    .append("Files Count", 1)
                    .append("CountTotalWrites", (Integer) 1)
                    .append("CountTotalReads", (Integer) 0)
                    .append("Total Files Storage Space in KB", fileSize)
                    .append("Average File Size", fileSize)
                    .append("FilesCollection lastUpdated", lastUpdated);
            statsCollection.insertOne(statsDoc);
            return;
        }

        /*
        Otherwise retrieve existing statsDocument and update it
         */
        Bson filter = eq("Name","Statistics Document");
        Document statsDoc = statsCollection.find(filter).first();
        int filesCount = (int) statsDoc.get("Files Count") + 1;
        Double totalFileSizes = (Double) statsDoc.get("Total Files Storage Space in KB") + fileSize;
        int totalWritesCount = (int) statsDoc.get("CountTotalWrites") + 1;
        Double avgFileSize = totalFileSizes / filesCount;

        Bson update1 = set("Files Count", filesCount);
        Bson update2 = set("Total Files Storage Space in KB", totalFileSizes);
        Bson update3 = set("CountTotalWrites", totalWritesCount);
        Bson update4 = set("Average File Size", avgFileSize);
        Bson update5 = set("FilesCollection lastUpdated", lastUpdated);
        Bson updateOperations = combine(update1, update2, update3, update4, update5);
        statsCollection.findOneAndUpdate(filter, updateOperations);
    }

}
