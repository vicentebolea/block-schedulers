#!/bin/env python

MIN_BLOCK_SIZE = 8 # MiB
CHUNK_SIZE = 4     # MiB

#  Generate size of logical chunk
def block_size(N, N_SLOTS, CHUNK_SIZE):
  return N/N_SLOTS * CHUNK_SIZE

# Split the chunks into two equal sets of chunks
def split_chunks(CHUNKS):
  A = dict(CHUNKS.items()[len(CHUNKS)/2:])
  B = dict(CHUNKS.items()[:len(CHUNKS)/2:])
  return A, B

# Build logical blocks for each wave
def assign_chunks_to_slots(NODES, SLOTS, CHUNKS):
  wave_slots = dict((k, [ ]) for k in SLOTS.keys())

  for index, replicas in CHUNKS.iteritems():
    emptiest_slot = min(map(lambda k: [len(wave_slots[k]), k], replicas), key=lambda a: a[0])
    wave_slots[emptiest_slot[1]].append(index)

  for k,v in wave_slots.iteritems():
    SLOTS[k].append(v)

# Split the file chunks in each wave
def schedule(NODES, SLOTS, CHUNKS):
  if block_size(len(CHUNKS), len(SLOTS), CHUNK_SIZE) < MIN_BLOCK_SIZE:
    return False

  first_half, second_half = split_chunks(CHUNKS)

  if not schedule(NODES, SLOTS, second_half):
    first_half.update(second_half)

  assign_chunks_to_slots(NODES, SLOTS, first_half)

  return True
