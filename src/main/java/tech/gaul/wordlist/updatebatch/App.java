package tech.gaul.wordlist.updatebatch;

import java.util.Optional;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import software.amazon.awssdk.services.sqs.SqsClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.client.OpenAIClient;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import tech.gaul.wordlist.updatebatch.models.ActiveBatchRequest;

public class App implements RequestHandler<SQSEvent, Object> {

    ObjectMapper objectMapper = new ObjectMapper();
    OpenAIClient openAIClient = DependencyFactory.getOpenAIClient();
    DynamoDbEnhancedClient dbClient = DependencyFactory.dynamoDbClient();
    Optional<String> emptyRequest = Optional.empty();

    SqsClient sqsClient = DependencyFactory.sqsClient();

    @Override
    public Object handleRequest(SQSEvent event, Context context) {

        BatchStatusUpdater batchStatusUpdater = BatchStatusUpdater.builder()
        .logger(context.getLogger())
                .sqsClient(sqsClient)
                .openAIClient(openAIClient)
                .dbClient(dbClient)                            
                .build();

        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .map(str -> {
                    try {
                        return Optional.of(objectMapper.readValue(str, ActiveBatchRequest.class).getBatchRequestId());
                    } catch (Exception e) {
                        return emptyRequest;
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::orElseThrow)
                .forEach(batchStatusUpdater::updateBatchStatus);

        return null;
    }
}