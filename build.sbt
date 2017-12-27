import com.typesafe.sbt.SbtGit

scalaVersion in ThisBuild := "2.11.12"

enablePlugins(GitPlugin)

organization := "io.bigpanda"
name := "kafka-helpers"
version := SbtGit.GitKeys.gitDescribedVersion.value.getOrElse("SNAPSHOT")

libraryDependencies ++= Seq(
  "org.apache.kafka"         % "kafka-clients"             % "0.10.2.1",
  "org.apache.curator"       % "curator-framework"         % "2.10.0",
  "org.apache.curator"       % "curator-recipes"           % "2.10.0",
  "org.apache.curator"       % "curator-test"              % "2.10.0",
  "com.typesafe.akka"        %% "akka-stream-kafka"        % "0.16",
  "net.manub"                %% "scalatest-embedded-kafka" % "0.13.1" % "test" exclude ("log4j", "log4j"),
  "org.apache.kafka"         %% "kafka"                    % "0.10.2.1" % "test" exclude ("org.slf4j", "slf4j-log4j12"),
  "com.typesafe.akka"        %% "akka-actor"               % "2.4.20",
  "com.typesafe.akka"        %% "akka-stream"              % "2.4.20",
  "com.typesafe.akka"        %% "akka-testkit"             % "2.4.20" % "test",
  "com.typesafe.akka"        %% "akka-stream-testkit"      % "2.4.20" % "test",
  "org.typelevel"            %% "cats-core"                % "0.9.0",
  "com.typesafe.play"        %% "play-json"                % "2.6.7",
  "org.apache.logging.log4j" % "log4j-core"                % "2.8.2",
  "org.apache.logging.log4j" % "log4j-api"                 % "2.8.2",
  "org.apache.logging.log4j" % "log4j-slf4j-impl"          % "2.8.2",
  "org.scalatest"            %% "scalatest"                % "3.0.0" % "test"
)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4")
scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Xfatal-warnings",
  "-Ypartial-unification"
)
testOptions += Tests.Argument("-oDF")
fork in (Test, test) := true
javaOptions in test ++= Seq("-Xms2G", "-Xmx2G")

wartremoverErrors ++= Seq(
  Wart.Any2StringAdd,
  Wart.EitherProjectionPartial,
  Wart.ListOps,
  Wart.Null,
  Wart.OptionPartial,
  Wart.Return,
  Wart.TryPartial,
  Wart.While)
wartremoverExcluded ++= PathFinder((sourceDirectories in Test).value).***.get

scalafmtVersion in ThisBuild := "1.2.0"
scalafmtConfig in ThisBuild := file(".scalafmt.conf")
scalafmtOnCompile in ThisBuild := true

licenses += ("Apache-2.0", url("https://opensource.org/licenses/Apache-2.0"))
homepage := Some(url("https://github.com/bigpandaio/kafka-helpers"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/bigpandaio/kafka-helpers"),
    "scm:git@github.com:bigpandaio/kafka-helpers.git"
  )
)

bintrayOrganization := Some("bigpanda")
bintrayRepository := "oss-maven"
bintrayPackageLabels := Seq("kafka", "scala", "akka-streams", "play-json")
