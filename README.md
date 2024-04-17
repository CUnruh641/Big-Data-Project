# MapReduce with Maven

Core source from <https://github.com/bandiajay/MapReduce>; this project builds the map reducer with `maven` instead of eclipse and ensures that it works with the newest version of winutils (which may be necessary for Apache Beam; we'll see).

## Prerequisites

Clone or download this repository

### Install Maven

You can [download](https://maven.apache.org/download.cgi) install maven (a Java build system) using [their instructions](https://maven.apache.org/install.html).
On Windows I found it easiest to use [Chocolatey](https://chocolatey.org/) because I'm lazy.

_Note_: If your package manager has maven 3.6.3 as the latest version, you cannot use Java 17.  Only the latest release of maven (3.8.6 at the time of writing this) supports OpenJDK 17.

### Install `winutils` (Windows Only)

This version of the sample MapReduce demo uses Hadoop 3.2.2, so you need to use that version of [`winutils`](https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.2/bin).

For the purposes of this document, I will assume that you are putting the required files in `C:\hadoop\bin`.  You may choose another directory, but the parent directory name of these files MUST be `bin`.

* Download `winutils.exe` and `hadoop.dll` and place them in `C:\hadoop\bin`
* To your _System_ environment variables, add a new variable named `Hadoop_Home` with the value `C:\hadoop`.  If you are unsure how to change environment variables on your version of Windows, use the search engine of your choice to find instructions.
* Add `C:\hadoop\bin` to your System `PATH` environment variable

## Running the Map Reducer

Open a terminal (On Windows, I've tested `PowerShell`, but I believe `cmd` should work; other OSes should be able to use your favorite terminal emulator) in this directory.
Ensure that the `data/output` directory doesn't exist (delete it if it does) then run one of the following depending on your OS:

* Windows: `mvn clean compile exec:java '-Dexec.mainClass="com.mycompany.app.ViewCount"' '-Dexec.args="data/input/rawViews.txt data/output"'`
* Other OSes (probably; untested right now): `mvn clean compile exec:java -Dexec.mainClass="com.mycompany.app.ViewCount" -Dexec.args="data/input/rawViews.txt data/output"` (tested on WSL Ubuntu 20.04 LTS)

For some reason `PowerShell` needs the single quotes around the `-Dexec` options.

You should see a new folder in the `data` directory; your output should now be in `data/output/part-r-00000`
