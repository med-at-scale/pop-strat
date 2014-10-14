Population stratification with ADAM
-----------------------------------

## Data sets from 1000genomes.

List of 1000genomes samples (with population) is given in:
ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20110521/phase1_integrated_calls.20101123.ALL.panel

This file is used as the source for population and is used to extract sample subsets (e.g. extract 5 sample ids per population).


Varations subsets are extracted from the 1000genomes browser (). 
Examples of regions selections

6:133017695-133161157 => 3481 variations

6:133017695-133031157 =>  292 variations

6:133017695-133019157 =>   41 variations

6:133017695-133017814 =>    5 variations


VCF files are obtained from the 1000genomes data browser at http://browser.1000genomes.org/Homo_sapiens/Info/Index


## Deploy on spark-notebook on ec2

Need the deps on the rigth spark, in the `project/buill.scala` file:
```{scala}
      unmanagedJars in Compile  += file("/root/spark/lib/datanucleus-api-jdo-3.2.1.jar"),
      unmanagedJars in Compile  += file("/root/spark/lib/datanucleus-core-3.2.2.jar"),
      unmanagedJars in Compile  += file("/root/spark/lib/datanucleus-rdbms-3.2.1.jar"),
      unmanagedJars in Compile  += file("/root/spark/lib/spark-assembly-1.1.0-hadoop2.0.0-mr1-cdh4.2.0.jar")
```

In the notebook, import ADAM deps from `central` using
```{scala}
resolveAndAddToJars("org.bdgenomics.adam" , "adam-core" , "0.14.0")
```
Since it will import `spark-core` and so on, we need to clean it from the jars added to the `SparkContext`.
```{scala}
jars = jars.filterNot(_.contains("spark-"))
// DITTO in the :cp block!
reset()
```

