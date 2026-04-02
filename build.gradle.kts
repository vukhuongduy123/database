plugins {
    id("java")
    id("com.diffplug.spotless") version "6.25.0"
}

group = "com.duyvu.database"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.projectlombok:lombok:1.18.40")
    annotationProcessor("org.projectlombok:lombok:1.18.40")

    testCompileOnly("org.projectlombok:lombok:1.18.40")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.40")

    implementation("org.apache.logging.log4j:log4j-api:2.25.3")
    implementation("org.apache.logging.log4j:log4j-core:2.25.3")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

// Spotless configuration
spotless {
    java {
        googleJavaFormat("1.17.0")
        target("src/**/*.java")
    }

    kotlinGradle {
        target("*.kts", "gradle/**/*.kts")
        ktlint("1.0.1")
    }
}
