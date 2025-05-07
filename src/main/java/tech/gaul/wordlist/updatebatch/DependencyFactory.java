package tech.gaul.wordlist.updatebatch;

import software.amazon.awssdk.regions.Region;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

public class DependencyFactory {

    private DependencyFactory() {
    }

    /**
     * @return an instance of SqsClient
     */
    public static SqsClient sqsClient() {
        return SqsClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .region(Region.of(System.getenv(SdkSystemSetting.AWS_REGION.environmentVariable())))
                .httpClientBuilder(AwsCrtHttpClient.builder())
                .build();
    }

    public static DynamoDbEnhancedClient dynamoDbClient() {
        DynamoDbClient dbClient = DynamoDbClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .region(Region.of(System.getenv(SdkSystemSetting.AWS_REGION.environmentVariable())))
                .httpClientBuilder(AwsCrtHttpClient.builder())
                .build();

        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dbClient)
                .build();
    }

    public static OpenAIClient getOpenAIClient() {
        return OpenAIOkHttpClient.builder()
                .apiKey(System.getenv("OPENAI_API_KEY"))
                .build();
    }
}
