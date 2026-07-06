plugins {
    id("java")
    id("com.diffplug.spotless") version "7.2.1"
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
    implementation("org.apache.logging.log4j:log4j-api:2.26.0")
    implementation("org.apache.logging.log4j:log4j-core:2.26.0")
    implementation("com.google.mug:dot-parse:10.5")
    implementation(platform("tools.jackson:jackson-bom:3.1.4"))
    implementation("tools.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.core:jackson-annotations")

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
        removeUnusedImports()
        googleJavaFormat("1.35.0")
        target("src/**/*.java")
        endWithNewline()
        lineEndings = com.diffplug.spotless.LineEnding.UNIX
    }
}
