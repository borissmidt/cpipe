#!/bin/zsh
./gradlew jar
# we could do more in the predef code to pre initialize the code. or load a config from file
amm --predef-code 'interp.load.cp(ammonite.ops.Path("/home/boris/IdeaProjects/cpipe2/build/libs/cpipe2-0.0.1-SNAPSHOT-all.jar"))'