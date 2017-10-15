package de.unistuttgart.ipvs.dds;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import de.unistuttgart.ipvs.dds.avro.DeviceRegistration;
import de.unistuttgart.ipvs.dds.avro.SensorRegistration;
import de.unistuttgart.ipvs.dds.avro.SensorTypeRegistration;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ObjectRegistrar {
    private final static Logger logger = LogManager.getLogger(ObjectRegistrar.class);

    private final static String DEVICE_REGISTRATION_TOPIC = "device-registration";
    private final static String SENSOR_REGISTRATION_TOPIC = "sensor-registration";
    private final static String SENSOR_TYPE_REGISTRATION_TOPIC = "sensor-type-registration";

    private static void printUsageAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("object-registrar", options);
        System.exit(1);
    }

    public static void main(String[] args) {
        /* Client ID with which to register with Kafka */
        final String clientId = "register-object";

        Options options = new Options();
        options.addOption("b", "bootstrap-servers", true, "Kafka connection string");
        options.addOption("s", "schema-registry", true, "URL of the schema registry");
        options.addRequiredOption("t", "type", true, "the type of object. Can be one of 'device', 'sensor', or 'sensor-type'.");
        options.addRequiredOption("i", "id", true, "the id of the object");
        options.addRequiredOption("n", "name", true, "the name of the object");
        options.addOption("d", "description", true, "the optional description ot the object");
        options.addOption("l", "location", true, "the location of the device");
        options.addOption("D", "device-id", true, "the id of the device the sensor belongs to. Required for sensor registration.");
        options.addOption("T", "sensor-type-id", true, "the id of the sensor's type. Required for sensor registration.");
        options.addOption("u", "unit", true, "the sensor type's unit of measurement");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);

            final String bootstrapServers = commandLine.getOptionValue("bootstrap-servers", "localhost:9092");
            final String schemaRegistry = commandLine.getOptionValue("schema-registry", "http://localhost:8081");

            final Properties producerProperties = new Properties();
            producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

            switch (commandLine.getOptionValue("type")) {
                case "device":
                    registerDevice(producerProperties, schemaRegistry, commandLine);
                    break;
                case "sensor":
                    registerSensor(producerProperties, schemaRegistry, commandLine, options);
                    break;
                case "sensor-type":
                    registerSensorType(producerProperties, schemaRegistry, commandLine);
                    break;
                default:
                    printUsageAndExit(options);
            }
        } catch (ParseException e) {
            logger.error("Failed to parse command line arguments", e);
            printUsageAndExit(options);
        }
    }

    private static void registerDevice(Properties producerProperties, String schemaRegistry, CommandLine commandLine) {
        final SpecificAvroSerde<DeviceRegistration> registrationSerde = createSerde(schemaRegistry);

        final String deviceId = commandLine.getOptionValue("id");
        final DeviceRegistration registration = new DeviceRegistration(
                commandLine.getOptionValue("name"),
                commandLine.getOptionValue("description", ""),
                commandLine.getOptionValue("location", "")
        );

        logger.info("Registering device " + deviceId + " as " + registration.toString());
        try (KafkaProducer<String, DeviceRegistration> producer = new KafkaProducer<>(
                producerProperties,
                Serdes.String().serializer(),
                registrationSerde.serializer()
        )) {
            // Wait for the future to complete
            producer.send(new ProducerRecord<>(DEVICE_REGISTRATION_TOPIC, deviceId, registration)).get();
        } catch (Exception e) {
            logger.error("Device registration failed", e);
        }
    }

    private static void registerSensor(Properties producerProperties, String schemaRegistry, CommandLine commandLine, Options options) {
        final SpecificAvroSerde<SensorRegistration> registrationSerde = createSerde(schemaRegistry);

        if (!commandLine.hasOption("device-id") || !commandLine.hasOption("sensor-type-id")) {
            printUsageAndExit(options);
        }

        final String sensorId = commandLine.getOptionValue("id");
        final SensorRegistration registration = new SensorRegistration(
                commandLine.getOptionValue("name"),
                commandLine.getOptionValue("description", ""),
                commandLine.getOptionValue("device-id"),
                commandLine.getOptionValue("sensor-type-id")
        );

        logger.info("Registering sensor " + sensorId + " as " + registration.toString());
        try (KafkaProducer<String, SensorRegistration> producer = new KafkaProducer<>(
                producerProperties,
                Serdes.String().serializer(),
                registrationSerde.serializer()
        )) {
            // Wait for the future to complete
            producer.send(new ProducerRecord<>(SENSOR_REGISTRATION_TOPIC, sensorId, registration)).get();
        } catch (Exception e) {
            logger.error("Sensor registration failed", e);
        }
    }

    private static void registerSensorType(Properties producerProperties, String schemaRegistry, CommandLine commandLine) {
        final SpecificAvroSerde<SensorTypeRegistration> registrationSerde = createSerde(schemaRegistry);

        final String typeId = commandLine.getOptionValue("id");
        final SensorTypeRegistration registration = new SensorTypeRegistration(
                commandLine.getOptionValue("name"),
                commandLine.getOptionValue("description", ""),
                commandLine.getOptionValue("unit", "")
        );

        logger.info("Registering sensor type " + typeId + " as " + registration.toString());
        try (KafkaProducer<String, SensorTypeRegistration> producer = new KafkaProducer<>(
                producerProperties,
                Serdes.String().serializer(),
                registrationSerde.serializer()
        )) {
            // Wait for the future to complete
            producer.send(new ProducerRecord<>(SENSOR_TYPE_REGISTRATION_TOPIC, typeId, registration)).get();
        } catch (Exception e) {
            logger.error("Sensor type registration failed", e);
        }
    }

    /**
     * Creates a serialiser/deserialiser for the given type, registering the Avro schema with the schema registry.
     *
     * @param schemaRegistryUrl the schema registry to register the schema with
     * @param <T>               the type for which to create the serialiser/deserialiser
     * @return                  the matching serialiser/deserialiser
     */
    private static <T extends SpecificRecord> SpecificAvroSerde<T> createSerde(final String schemaRegistryUrl) {
        final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl
        );
        serde.configure(serdeConfig, false);
        return serde;
    }
}
