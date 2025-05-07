package tech.gaul.wordlist.updatebatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.client.OpenAIClient;
import com.openai.core.http.HttpResponse;
import com.openai.models.batches.Batch;
import com.openai.models.batches.BatchRetrieveParams;
import com.openai.models.batches.Batch.Status;
import com.openai.models.files.FileContentParams;

import lombok.Builder;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import tech.gaul.wordlist.updatebatch.models.ActiveBatchRequest;
import tech.gaul.wordlist.updatebatch.models.ActiveWordQuery;
import tech.gaul.wordlist.updatebatch.models.BatchQueryResponse;
import tech.gaul.wordlist.updatebatch.models.QueryWordMessage;
import tech.gaul.wordlist.updatebatch.models.UpdateWordMessage;

@Builder
public class BatchStatusUpdater {

    private final LambdaLogger logger;
    private final SqsClient sqsClient;
    private final OpenAIClient openAIClient;
    private final DynamoDbEnhancedClient dbClient;

    final ObjectMapper objectMapper = new ObjectMapper();
    final Optional<BatchQueryResponse> emptyBatchQueryResponse = Optional.empty();
    final Optional<UpdateWordMessage> emptyUpdateWordMessage = Optional.empty();
    final Optional<SendMessageBatchRequestEntry> emptySendMessageBatchRequestEntry = Optional.empty();

    final TableSchema<ActiveBatchRequest> activeBatchRequestSchema = TableSchema.fromBean(ActiveBatchRequest.class);
    final TableSchema<ActiveWordQuery> activeWordQuerySchema = TableSchema.fromBean(ActiveWordQuery.class);

    /**
     * Updates the status of a batch request.
     *
     * @param activeBatchRequestId The ID of the active batch request to update.
     * @return true if the active request can be deleted because the request was
     *         successful, or a permanent error occurred.
     */
    private boolean doUpdateBatchStatus(String activeBatchRequestId) {

        ActiveBatchRequest activeBatchRequest = dbClient.table(System.getenv("ACTIVE_BATCHES_TABLE_NAME"), activeBatchRequestSchema)
                .getItem(r -> r.key(k -> k.partitionValue(activeBatchRequestId)));

        if (activeBatchRequest == null) {
            logger.log("Batch request not found: " + activeBatchRequestId);
            return true;
        }

        BatchRetrieveParams batchRetrieveParams = BatchRetrieveParams.builder()
                .batchId(activeBatchRequest.getBatchRequestId())
                .build();

        Batch batch = openAIClient.batches().retrieve(batchRetrieveParams);

        if (batch == null) {
            logger.log("Batch not found: " + activeBatchRequestId);
            return true;
        }

        if (!batch.status().equals(Status.COMPLETED)) {
            logger.log("Skipped batch " + activeBatchRequestId + " with invalid status: " + batch.status());
            return false;
        }

        logger.log("Retrieving word queries for batch: " + activeBatchRequestId);

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder()
                .filterExpression(Expression.builder()
                        .expression("batchRequestId = :batchRequestId")
                        .expressionValues(
                                Map.of(":batchRequestId", AttributeValue.fromS(activeBatchRequest.getBatchRequestId())))
                        .build())
                .build();

        Map<String, ActiveWordQuery> activeWordQueries = dbClient.table(System.getenv("ACTIVE_QUERIES_TABLE_NAME"), activeWordQuerySchema)
                .scan(scanRequest)
                .items()
                .stream()
                .collect(Collectors.toMap(ActiveWordQuery::getWord, activeWordQuery -> activeWordQuery));

        logger.log("Found " + activeWordQueries.size() + " word queries for batch: " + activeBatchRequestId);

        // Get the response data. We are expecting a single JSON object for each word in
        // the response.
        String outputFileId = batch.outputFileId().orElseThrow();

        FileContentParams fileContentParams = FileContentParams.builder()
                .fileId(outputFileId)
                .build();

        HttpResponse contentResponse = openAIClient.files().content(fileContentParams);
        if (contentResponse == null) {
            logger.log("Failed to retrieve content for batch: " + activeBatchRequestId);
            return false;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(contentResponse.body()));

        // Build a stream of UpdateWordMessage objects to send from the LLM response.
        Stream<UpdateWordMessage> updateMessages = reader.lines()
                .map(line -> {
                    try {
                        return Optional.of(objectMapper.readValue(line, BatchQueryResponse.class));
                    } catch (IOException e) {
                        logger.log("Failed to parse line: " + line);
                        return emptyBatchQueryResponse;
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(queryResponse -> {
                    String word = queryResponse.getWord();
                    ActiveWordQuery activeWordQuery = activeWordQueries.get(word);
                    // Only include messages which we actually queried for.
                    if (activeWordQuery != null) {
                        return Optional.of(UpdateWordMessage.builder()
                                .word(word)
                                .commonness(queryResponse.getCommonness())
                                .offensiveness(queryResponse.getOffensiveness())
                                .sentiment(queryResponse.getSentiment())
                                .types(queryResponse.getTypes())
                                .build());
                    } else {
                        return emptyUpdateWordMessage;
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get);

        // Send the update messages to the SQS queue.
        BatchingIterator.batchedStreamOf(updateMessages, 10)
                .forEach(batchedMessages -> {
                    SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                            .queueUrl(System.getenv("UPDATE_WORD_QUEUE_URL"))
                            .entries(batchedMessages.stream()
                                    .map(message -> {
                                        try {
                                            return Optional.of(SendMessageBatchRequestEntry.builder()
                                                    .id(message.getWord())
                                                    .messageBody(objectMapper.writeValueAsString(message))
                                                    .build());
                                        } catch (Exception e) {
                                            logger.log("Failed to serialize message: " + message);
                                            return emptySendMessageBatchRequestEntry;
                                        }
                                    })
                                    .filter(Optional::isPresent)
                                    .map(Optional::get)
                                    .collect(Collectors.toList()))
                            .build();
                    sqsClient.sendMessageBatch(sendMessageBatchRequest);
                });

        // Send re-request messages for any words which we did not get a response for.
        activeWordQueries.keySet().stream()
                .filter(word -> !updateMessages.anyMatch(message -> message.getWord().equals(word)))
                .forEach(word -> {
                    logger.log("Re-requesting word: " + word);
                    QueryWordMessage queryWordMessage = QueryWordMessage.builder()
                            .word(word)
                            .force(true) // If we made it this far, we definitely want to update the word.
                            .build();

                    String messageBody;
                    try {
                        messageBody = objectMapper.writeValueAsString(queryWordMessage);
                        SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                                .queueUrl(System.getenv("QUERY_WORD_QUEUE_URL"))
                                .entries(List.of(SendMessageBatchRequestEntry.builder()
                                        .id(word)
                                        .messageBody(messageBody)
                                        .build()))
                                .build();
                        sqsClient.sendMessageBatch(sendMessageBatchRequest);
                    } catch (Exception e) {
                        logger.log("Failed to convert message for word '" + word + "' to JSON.");
                        logger.log("Error: " + e.toString());
                    }
                });

        logger.log("Batch request completed: " + activeBatchRequestId);

        return true;

    }

    public void updateBatchStatus(String activeBatchRequestId) {
        logger.log("Updating batch status for: " + activeBatchRequestId);
        boolean shouldDelete = doUpdateBatchStatus(activeBatchRequestId);
        if (shouldDelete) {
            logger.log("Deleting batch request: " + activeBatchRequestId);
            dbClient.table(System.getenv("ACTIVE_BATCHES_TABLE_NAME"), activeBatchRequestSchema)
                    .deleteItem(r -> r.key(k -> k.partitionValue(activeBatchRequestId)));
        }
    }
}
