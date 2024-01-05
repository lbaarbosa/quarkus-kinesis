package poc.quarkus.kinesis.model;

public record Notification(String name, String uuid, Integer timestamp) {
}
