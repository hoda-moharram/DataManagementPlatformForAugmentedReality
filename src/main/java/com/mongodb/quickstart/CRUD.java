package com.mongodb.quickstart;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

public class CRUD {
    /*
    Here a printWriter is instantiated once to be used for all CRUD operations and keep the log file up to date
    with successful and unsuccessful operations and the reason behind failures
     */
    static PrintWriter out;
    static {
        try {
            out = new PrintWriter(new FileOutputStream("data/LogFile.txt", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Returns the filesCollection from the MongoDB
    protected static MongoCollection<Document> getFilesCollection(){
        MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"));
        MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
        return sampleTrainingDB.getCollection("ARObjectsDatabase");
    }

    // Returns the statsCollection from the MongoDB
    protected static MongoCollection<Document> getStatsCollection(){
        MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"));
        MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
        return sampleTrainingDB.getCollection("AR_DB_Stats");
    }

    // String manipulation to retrieve fileName from filePath
    protected static String getFileName(String filePath){
        int lastSlash = filePath.lastIndexOf("/");
        String fileName = filePath.substring(lastSlash + 1, filePath.length());
        return fileName;
    }

    // Retrieves the file size of files using the filePaths
    protected static Double getFileSizeKiloBytes(File file) {
        return (double) file.length() / 1024;
    }
}
