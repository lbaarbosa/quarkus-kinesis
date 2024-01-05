package poc.quarkus.kinesis.controller;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import poc.quarkus.kinesis.aws.AWSKinesisClient;
import poc.quarkus.kinesis.model.Notification;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Path("/quarkus-kinesis")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Test controller", description = "Quarkus-Kinesis integration controller for testing purposes")
public class KinesisTestController {

    Random random = new Random();
    String partitionKey = UUID.randomUUID().toString();
    List<String> notificationList = new ArrayList<>();

    @GET
    @Path("/500")
    @Operation(summary = "This request sends 500 records at once with a single API call request")
    public Response send500recordsAtOnce() {
        KinesisTestController mainApp = new KinesisTestController();
        mainApp.populateNotificationList();
        String streamName = "kinesis-integration-lambda-test";

        //To send data to Kinesis, we must execute the following steps:

        //1. Instantiate a KinesisClient object
        AmazonKinesis amazonKinesis = AWSKinesisClient.getAmazonKinesisClient();

        //2. Create a PutRecordsRequest object
        List<PutRecordsRequestEntry> requestEntryList = mainApp.getPutRecordsRequestEntryList();
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        putRecordsRequest.setRecords(requestEntryList);

        //3. Call the putRecords method of the KinesisClient object (up to 500 records with a single API call)
        PutRecordsResult results = amazonKinesis.putRecords(putRecordsRequest);
        log.info("PutRecordsResult -> " + results);


        return Response.ok().build();
    }

    @GET
    @Path("/1")
    @Operation(summary = "This request sends 1 record to Kinesis Data Stream")
    public Response send1recordPerRequest() {
        KinesisTestController mainApp = new KinesisTestController();
        mainApp.populateNotificationList();
        String streamName = "kinesis-integration-lambda-test";

        //To send data to Kinesis, we must execute the following steps:

        //1. Instantiate a KinesisClient object
        AmazonKinesis amazonKinesis = AWSKinesisClient.getAmazonKinesisClient();

        //2. Create a PutRecordRequest object
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);

        Notification notification = new Notification(
                "A new feature is available",
                UUID.randomUUID().toString(),
                random.nextInt(9999)
        );

        putRecordRequest.setData(ByteBuffer.wrap(gson.toJson(notification).getBytes()));
        putRecordRequest.setPartitionKey(partitionKey);

        //3. Call the putRecord method of the KinesisClient object
        PutRecordResult result = amazonKinesis.putRecord(putRecordRequest);
        log.info("PutRecordResult -> " + result);

        return Response.ok().build();
    }

    @GET
    @Path("/get")
    @Operation(summary = "This request retrieves data from Kinesis Data Stream")
    public Response getRecords() {
        KinesisTestController mainApp = new KinesisTestController();
        mainApp.populateNotificationList();
        String streamName = "kinesis-integration-lambda-test";
        String shardId = "shardId-000000000001";
        String sequenceNumber = "49647769585298572699646905094020095640747936436373684242";

        //To send data to Kinesis, we must execute the following steps:

        //1. Instantiate a KinesisClient object
        AmazonKinesis amazonKinesis = AWSKinesisClient.getAmazonKinesisClient();

        //2. Create a GetRecordsRequest object

        //2.1 Create a GetShardIteratorRequest
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                .withStreamName(streamName)
                .withShardId(shardId)
                .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .withStartingSequenceNumber(sequenceNumber);

        //2.2 Get the shard iterator
        GetShardIteratorResult getShardIteratorResult = amazonKinesis.getShardIterator(getShardIteratorRequest);
        String shardIterator = getShardIteratorResult.getShardIterator();

        GetRecordsRequest getRecordsRequest = new GetRecordsRequest()
                .withShardIterator(shardIterator)
                .withLimit(100);

        //3. Call the putRecord method of the KinesisClient object
        GetRecordsResult result = amazonKinesis.getRecords(getRecordsRequest);
        List<Record> records = result.getRecords();

        //4. Process the records
        for (Record record : records) {
            // Retrieve data from the record
            ByteBuffer data = record.getData();
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            String recordData = new String(bytes);
            log.info("Record data -> " + recordData);
        }
        log.info("Here are the records retrieved -> " + records);
        return Response.ok().build();
    }

    private List<PutRecordsRequestEntry> getPutRecordsRequestEntryList() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();
        for (Notification notification : getNotificationList()) {
            PutRecordsRequestEntry requestEntry = new PutRecordsRequestEntry();
            requestEntry.setData(ByteBuffer.wrap(gson.toJson(notification).getBytes()));
            requestEntry.setPartitionKey(partitionKey);
            putRecordsRequestEntries.add(requestEntry);
        }
        return putRecordsRequestEntries;
    }

    private List<Notification> getNotificationList() {
        List<Notification> notifications = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            notifications.add(
                    new Notification(
                            notificationList.get(random.nextInt(notificationList.size())),
                            UUID.randomUUID().toString(),
                            random.nextInt(999999)
                    )
            );
        }
        return notifications;
    }

    private void populateNotificationList() {
        notificationList.add("New content available for IPA");
        notificationList.add("Scheduled maintenance coming");
        notificationList.add("Festival in the city happening");
        notificationList.add("New album released by your most listened to artist");
        notificationList.add("Best driving mode for this route");
        notificationList.add("A feature that has not yet been tested");
        notificationList.add("Your football team will play in the Bundesliga today");
        notificationList.add("There is a recall for your car model");
    }

}
