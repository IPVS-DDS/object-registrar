FROM openjdk:8

ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/register-object/register-object.jar"]

# Add Maven dependencies
ADD target/lib /usr/share/register-object/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/register-object/register-object.jar
