# Scala Base project

## Instructions to run

https://docs.scala-lang.org/getting-started/intellij-track/building-a-scala-project-with-intellij-and-sbt.html

## Running the project

### 1. From the Run menu, select Edit configurations

### 2. Click the + button and select sbt Task.

### 3. Name it ```Run the program```.

### 4. In the Tasks field, type ```~run```. The ~ causes sbt to rebuild and rerun the project when you save changes to a file in the project.

### 5. Click ```OK```.

### 6. On the Run menu. Click Run ```Run the program```.

![image.png](assets/image.png)


## Pre-requisites

```
winutils.exe and hadoop.dll

```

since the project interacts with windows filesystem to read and write files, the two files need to be present in hadoop bin directory and environment variables need to be set.

If not, you can set the trigger mode in config file to ```printonly```

## Data locations

Input data location: 	src/main/Resources/input
Output data location: 	src/main/Resources/output

## Configurations

Properties file: 		src/main/Resources/properties.conf  (contains job configurations such as input and output locations and parameters)

excludeCountryForQ3 = "uk"
atLeastNTimesForQ4 = 3 // minimum number of time two passengers must have flown together to feature in result
outputMode = "both"   // can be set to either ```"fileonly", "printonly" or "both"```
