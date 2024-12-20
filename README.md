# SignalJ
A compiler of SignalJ programming language.

*This is the version used for the benchmark experiment in the paper "Consistent Distributed Reactive Programming with Retroactive Computation," submitted to the journal "The Art, Science, and Entineering of Programming." The latest version appears on the GitHub ...*

This comiler is developped on the basis of ExtendJ, an extensible compiler of Java (http://jastadd.org/web/extendj/). All tools needed are included in ExtendJ. To build our compiler, you only need to have javac and Apache Ant.

To build the SignalJ compiler, you need to follow the following instructions.

1. Download and extract ExtendJ compiler.
2. Copy the directory named "signalj" to the top-level dierctory of the extracted ExtendJ directory.
3. Change directory to the copied signalj directory.
4. Run "ant build jar". Then, the jar file "signalj.jar" will be created in the directory.

To run the compiler, you just need to run the following command.

$ java -jar signalj.jar [source files]

The subdirectory "runtime" contains the SignalJ runtime library, which is necessary to run the SignalJ program.  To run your program, it is necessary to include the following libraries in your classpath:

* This runtime library
* RxJava 2.x
* R2DBC (see https://r2dbc.io/)
* JDBC built for PostgreSQL

To use *persistent signals*, you are also required to set up TimescaleDB. You are also required to provide the following configuration information in the file /[path to your home directory]/signalj/properties/java.properties:

url=[The URL of your database containing update histories of signals]
user=[The database user name]
password=[Password for the user]
admin=[The database administrator user name. You can leave this blank]
adminpw=[Password for the administrator. You can leave this blank]
distributed=false

You can also use a directory service for distributed persistent signals. In this case, the configuration should be as follows (note that currenly there are no public directory service):

url=[The URL of directory service]
user=[User name for the directory service]
password=[Password for the directory service]
admin=[Leave this blank]
adminpw=[Leave this blank]
distributed=true

If all these instructions are not sufficient, please consult the README file on the GitHub, or contact kamina@oita-u.ac.jp.
