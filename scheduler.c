const int MIN_BLOCK_SIZE = 8 // MiB
const int CHUNK_SIZE = 4     // MiB

bool schedule(int SLOTS[], int CHUNKS[]) {
  if (LEN(CHUNKS)/LEN(SLOTS) * CHUNK_SIZE < MIN_BLOCK_SIZE) {
      return false;
  }

  int *C_1, *C_2;

  split_chunks(CHUNKS, &C_1, &C_2);

  if (!schedule(SLOTS, C_2)) {
     arr_append(&C_1, C_2);
  }

  assing_chunks_to_slots(SLOTS, C_1);

  return true;
}
