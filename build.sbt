name := "bob"

version := "0.0.1"

scalaVersion := "2.9.1"

libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % "1.1.0"

fork in run := true

autoCompilerPlugins := true

scalacOptions ++= Seq(
                    "-deprecation"
                  , "-unchecked"
                  , "-explaintypes"
                  , "-optimize"
                  //, "-Xlog-implicits"
                  //, "-Ydebug"
                  , "-Xlint"
                  )
