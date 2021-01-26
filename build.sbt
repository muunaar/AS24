name := "AS24"

version := "0.1"

scalaVersion := "2.13.4"

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
fork in run := true

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "io.chrisdavenport" %% "log4cats-slf4j" % "1.1.1",
  "com.github.gekomad" %% "itto-csv" % "1.1.0",
  "org.typelevel" %% "cats-effect" % "2.3.1",
  "de.vandermeer" % "asciitable" % "0.3.2",
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
)