plugins {
    id("java")
    id("com.diffplug.spotless") version "6.25.0"
    id("application")
}

group = "com.duyvu.database"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.projectlombok:lombok:1.18.44")
    annotationProcessor("org.projectlombok:lombok:1.18.44")

    testCompileOnly("org.projectlombok:lombok:1.18.44")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.44")

    implementation("org.apache.logging.log4j:log4j-api:3.0.0-beta3")
    implementation("org.apache.logging.log4j:log4j-core:3.0.0-beta3")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.duyvu.database.Main"
    }
    from(
        configurations.runtimeClasspath.get().map {
            if (it.isDirectory) it else zipTree(it)
        },
    )

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.named<Jar>("jar") {
    archiveFileName.set("app.jar")
}
// Spotless configuration
spotless {
    java {
        googleJavaFormat("1.35.0")
        target("src/**/*.java")
        endWithNewline()
        lineEndings = com.diffplug.spotless.LineEnding.UNIX
    }
}
