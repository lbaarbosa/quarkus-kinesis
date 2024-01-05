package poc.quarkus.kinesis.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

@ApplicationScoped
public class AWSKinesisClient {

    @Produces
    public static AmazonKinesis getAmazonKinesisClient() {
        AWSStaticCredentialsProvider credentialsProvider =
                new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        System.getenv("AWS_ACCESS_KEY"),
                        System.getenv("AWS_SECRET_KEY")
                ));
        return AmazonKinesisClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.EU_CENTRAL_1)
                .build();
    }

}
