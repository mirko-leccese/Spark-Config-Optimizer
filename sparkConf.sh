#!/bin/bash

# Prompt the user for input
read -p "Enter the number of worker nodes: " numWorkerNodes
read -p "Enter vCore per worker node: " numCoresWorkerNode
read -p "Enter RAM per worker node (in GB): " ramWorkerNode
read -p "Enter shuffle read (in GB): " shuffleRead
read -p "Enter vCore per master node: " numCoresMasterNode
read -p "Enter partition size (in MB): " partitionSize

# Perform calculations: parallelization
executorPerNode=$(bc <<< "($numCoresWorkerNode - 1) / 5")
executorInstances=$(bc <<< "($executorPerNode * ($numWorkerNodes + 1)) - 1")
totMemoryExecutor=$(bc <<< "($ramWorkerNode - 1) / $executorPerNode")
executorMemory=$(bc <<< "$totMemoryExecutor*0.9")
# Round executorMemory
roundedExecutorMemory=$( echo $( bc <<< "$executorMemory+1" ) | awk '{printf "%.0f",$0}')
driverMemory=$roundedExecutorMemory

sparkDefaultParallelism=$(bc <<< "5 * $executorInstances * 2")

# Perform calculations: partition settings
totSparkPartitions=$(bc <<< "$shuffleRead * 1024 / $partitionSize")
totCores=$(($numCoresWorkerNode * $numWorkerNodes + $numCoresMasterNode))
coreCycles=$(bc <<< "$totSparkPartitions / $totCores")
suggestedShufflePartitions=$(bc <<< "scale=0; $coreCycles * $totCores")
partitionBytes=$(($partitionSize * 1024 * 1024))

# Display the results
echo "{
  \"spark.executor.cores\": \"5\",
  \"spark.executor.instances\": \"$executorInstances\",
  \"spark.executor.memory\": \"$roundedExecutorMemory\",
  \"spark.driver.memory\": \"$driverMemory\",
  \"spark.default.parallelism\": \"$sparkDefaultParallelism\",
  \"spark.sql.shuffle.partitions\": \"$suggestedShufflePartitions\",
  \"spark.sql.files.maxPartitionBytes\": \"$partitionBytes\"
}"
