# Offset manager

Every kafka message has an incremental offset. Kafka API has method to commit offsets given as arguments. If a larger
offset is committed, lower offsets are considered to be automatically committed.

Offset manager is a data structure which calculates committable offsets for each partition.
To use offset manager:
* add message(s) with metadata about offset and partition with a batch key.
  * add offsets into a sorted map with committable flag to be false.
  * `addOffsetToBatch(Object batch, List<Message> messages)`
* set messages to be committable once the processing is finished.
  * `setCommittable(Object batch)` to set the committable flag to be true.
* `getCommittableOffset()` returns the largest offset that can be committed.

## Implementation
### Data Structures
* OffsetNode: A combination of topic, partition and the offset.
* toBeCommittableBatchOffsets: A map of batch-keys and a set of OffsetNodes.
* sortedOffsets: A map of topic-partition to a sorted list of OffsetNode.
### Adding offsets
When `addOffsetToBatch(Object batch, List<Message> messages)` is called, it creates a OffsetNode from the message.
Each Topic-Partition has a sorted list by offsets. The OffsetNode is added into this sorted list.
OffsetNode is also added into the map keyed by provided key.
### Setting a batch to be Committable.
`setCommittable(Object batch)` sets a flag `isCommittable` to be true on each
OffsetNode on the batch. It also removes from the map `toBeCommittableBatchOffsets`.
### Getting Committable offsets
`getCommittableOffset()`
* For each topic-partition:
  * Look for the contiguous offsets in the sorted list which are set to be committed.
  * Return the largest offset from the contiguous series.
  * Delete smaller OffsetNodes from the sorted list.

