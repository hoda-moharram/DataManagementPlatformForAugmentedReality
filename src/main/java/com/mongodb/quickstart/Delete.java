package com.mongodb.quickstart;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;

public class Delete {

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
            MongoCollection<Document> fileCollections = sampleTrainingDB.getCollection("Just work");

            // delete one document
            Bson filter = eq("_id", "60c3a2a9b228b916fed0aacc");
//            DeleteResult result = fileCollections.deleteOne(filter);
//            System.out.println(result);

            // findOneAndDelete operation
            filter = eq("FilePath", "St5");
//            Document doc =
            System.out.println(fileCollections.findOneAndDelete(filter));
//            System.out.println(doc.toJson(JsonWriterSettings.builder().indent(true).build()));

            // delete many documents
//            filter = gte("_id", "60c3a2a9b228b916fed0aad1");
//            result = fileCollections.deleteMany(filter);
//            System.out.println(result);

            // delete the entire collection and its metadata (indexes, chunk metadata, etc).
//            fileCollections.drop();
        }
    }

    public static boolean deleteRecord(String filePath){
        MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"));
        MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
        MongoCollection<Document> fileCollections = sampleTrainingDB.getCollection("Just work");

        Bson filter = eq("FilePath", filePath);
        if(fileCollections.countDocuments(filter) > 0)
            fileCollections.findOneAndDelete(filter);
        else
            return false;
        return true;
    }

    public static boolean deleteFile(String filePath){
        MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"));
        MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
        MongoCollection<Document> fileCollections = sampleTrainingDB.getCollection("Just work");

        Bson filter = eq("FilePath", filePath);
        if(fileCollections.countDocuments(filter) > 0){
            File file = new File(filePath);
            if(file.delete()){
                fileCollections.findOneAndDelete(filter);
                return true;
            }
            else{
                fileCollections.findOneAndDelete(filter);
                return false;
            }
        }
        return false;
    }

    public static boolean deleteAllRecords(){
        MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"));
        MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
        MongoCollection<Document> fileCollections = sampleTrainingDB.getCollection("Just work");
        fileCollections.drop();
        return true;
    }


//    public static boolean deleteAllFiles(){
//        MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"));
//        MongoDatabase sampleTrainingDB = mongoClient.getDatabase("sample_training");
//        MongoCollection<Document> fileCollections = sampleTrainingDB.getCollection("Just work");
//
//
//
//        Bson filter = eq("FilePath", filePath);
//        if(fileCollections.countDocuments(filter) > 0){
//            File file = new File(filePath);
//            if(file.delete()){
//                fileCollections.findOneAndDelete(filter);
//                return true;
//            }
//            else{
//                fileCollections.findOneAndDelete(filter);
//                return false;
//            }
//        }
//        return false;
//    }
}
