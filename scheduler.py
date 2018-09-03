#!/bin/env python

MIN_BLOCK_SIZE = 8 # MiB
CHUNK_SIZE = 4     # MiB

# Split the chunks into two equal sets of chunks
def split_chunks(CHUNKS):
  A = dict(CHUNKS.items()[len(CHUNKS)/2:])
  B = dict(CHUNKS.items()[:len(CHUNKS)/2])
  return A, B

# Build logical blocks for each wave
def assign_chunks_to_slots(SLOTS, CHUNKS):
  slots_dist = dict((k, [ ]) for k in SLOTS.keys())

  for chunk_id, replicas in CHUNKS.iteritems():
    empty_slot = min(replicas, key=lambda id: len(slots_dist[id]))
    slots_dist[empty_slot].append(chunk_id)

  for k, v in slots_dist.iteritems():
    SLOTS[k].append(v)

# Split the file chunks in each wave
def schedule(SLOTS, CHUNKS):
  if len(CHUNKS)/len(SLOTS) * CHUNK_SIZE < MIN_BLOCK_SIZE:
    return False

  C_1, C_2 = split_chunks(CHUNKS)

  if not schedule(SLOTS, C_2):
    C_1.update(C_2)

  assign_chunks_to_slots(SLOTS, C_1)

  return True
