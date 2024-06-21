plugins {
    `application`
}

repositories {
    mavenCentral()
}

application {
    mainClass.set("tech.ytsaurus.example.ExampleReduceYTree")
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    implementation(project(":yt:java:annotations"))
    implementation(project(":yt:java:ytsaurus-client"))
    implementation("javax.persistence:persistence-api:1.0")
    implementation("com.google.protobuf:protobuf-java:3.25.3")

    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.1") {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


