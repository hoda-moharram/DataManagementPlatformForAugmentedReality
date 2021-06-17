package com.mongodb.quickstart;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;

import java.io.*;
import java.util.*;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class Create {

    private static final Random rand = new Random();
    static PrintWriter out;

    static {
        try {
            out = new PrintWriter(new FileOutputStream("data/LogFile.txt", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> fileCollections = sampleTrainingDB.getCollection("Just work");
            String filePath = "C:/Users/Zeina Kandil/Downloads/ergonomic-bottle-1.snapshot.2/Bottle.obj";

            String filePath2 = "C:/Users/Zeina Kandil/Downloads/ergonomic-bottle-1.snapshot.2/Bottle.step";
//            insertOneDocument(filePath);
            ArrayList<String > filePaths = new ArrayList<>(); filePaths.add(filePath); filePaths.add(filePath2); filePaths.add(filePath2);
            insertManyDocuments(filePaths);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static MongoCollection<Document> getFilesCollection(){
        MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"));
        MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
        return sampleTrainingDB.getCollection("Just work");
    }

    private static void insertOneDocument(String filePath) throws FileNotFoundException {
        MongoCollection<Document> filesCollection = getFilesCollection();
        if(filesCollection.countDocuments(eq("FilePath", filePath)) == 0){
            Document document = saveNewFile(filePath);
            filesCollection.insertOne(document);
            System.out.println("One File inserted.");
        }
        else
            System.out.println("Previously Inserted. Cannot insert twice.");
    }

    private static void insertManyDocuments(ArrayList<String> filePaths) throws FileNotFoundException {
        HashSet<String> hsFilePaths = getValidFilePaths(filePaths);
        if(hsFilePaths.isEmpty())
            return;
        MongoCollection<Document> filesCollection = getFilesCollection();
        List<Document> files = new ArrayList<>();
        for (String filePath: hsFilePaths) {
            Document document = saveNewFile(filePath);
            files.add(document);
        }
        filesCollection.insertMany(files, new InsertManyOptions().ordered(false));
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
                out.println(filePath + " is an invalid file path and was not added to the database.");
                continue;
            }
            if(filesCollection.countDocuments(eq("FilePath", filePath)) == 0){
                if(hsFilePaths.contains(filePath)){
                    out.println(filePath + " was not added to the database as there is another file with the same file path in the same insertMany operation.");
                }else{
                    hsFilePaths.add(filePath);
                    out.println(filePath + " has been added to the database successfully");
                }
            }else{
                out.println(filePath + " was not added to the database as there is another file with the same file path.");
            }
        }
        out.flush();
        return hsFilePaths;
    }


    private static Document saveNewFile(String filePath){
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()){
            out.println(filePath + " is an invalid file Path and was not added to the database.");
        }
        String fileName = getFileName(filePath);
        String fileDescription = "No description";
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
        System.out.println(fileName + " " + fileSize + " " + date);
        return fileDoc;
    }

    private static String getFileName(String filePath){
        int lastSlash = filePath.lastIndexOf("/");
        String fileName = filePath.substring(lastSlash + 1, filePath.length());
        return fileName;
    }

    private static Double getFileSizeKiloBytes(File file) {
        return (double) file.length() / 1024;
    }
}
