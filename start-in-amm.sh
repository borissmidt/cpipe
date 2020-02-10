#!/bin/zsh
./gradlew jar
# we could do more in the predef code to pre initialize the code. or load a config from file
java -cp /home/boris/IdeaProjects/cpipe2/build/libs/cpipe2-0.0.1-SNAPSHOT-all.jar org.splink.cpipe.Run