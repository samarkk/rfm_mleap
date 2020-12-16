name := "MLeapSAProject"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "ml.combust.mleap" %% "mleap-runtime" % "0.16.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
