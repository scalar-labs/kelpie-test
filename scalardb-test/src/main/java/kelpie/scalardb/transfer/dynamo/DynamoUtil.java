package kelpie.scalardb.transfer.dynamo;

import com.scalar.kelpie.config.Config;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoUtil {

  public static DynamoDbClient createDynamoClient(Config config) {
    String region = config.getUserString("dynamo_config", "region");
    String accessKey = config.getUserString("dynamo_config", "access_key_id");
    String secretAccessKey = config.getUserString("dynamo_config", "secret_access_key");

    return DynamoDbClient.builder()
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretAccessKey)))
        .region(Region.of(region))
        .build();
  }
}
