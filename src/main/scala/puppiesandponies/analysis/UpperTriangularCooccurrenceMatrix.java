package puppiesandponies.analysis;

import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import it.unimi.dsi.fastutil.shorts.ShortIterator;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;

public class UpperTriangularCooccurrenceMatrix {

  private CooccurrenceMatrixRow rows[];

  public UpperTriangularCooccurrenceMatrix(int numItems, int initialCapacityPerItem) {
    this.rows = new CooccurrenceMatrixRow[numItems];
    for (int n = 0; n < numItems; n++) {
      rows[n] = new CooccurrenceMatrixRow(initialCapacityPerItem);
    }
  }

  public void observeCooccurrence(int itemA, int itemB) {
    rows[itemA].increment(itemB);
  }

  public int zeroNorm() {
    int nnz = 0;
    for (int n = 0; n < rows.length; n++) {
      nnz += rows[n].size();
    }
    return nnz;
  }

  public int oneNorm() {
    int norm = 0;
    for (int n = 0; n < rows.length; n++) {
      ShortIterator entries = rows[n].values().iterator();
      while (entries.hasNext()) {
        norm += entries.nextShort();
      }
    }
    return norm;
  }

  private class CooccurrenceMatrixRow extends Int2ShortOpenHashMap {

    public CooccurrenceMatrixRow(int initialCapacityPerItem) {
      super(initialCapacityPerItem);
    }

    /* copied from Int2ShortOpenHashMap */
//    public int insert(final int k, final short v) {
    public int increment(final int k) {
      int pos;
      if (((k) == (0))) {
        if (containsNullKey) {
          value[n] += 1; //SSC: increment in-place
          return n;
        }
        containsNullKey = true;
        pos = n;
      } else {
        int curr;
        final int[] key = this.key;
        // The starting point.
        if (!((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k)))
            & mask]) == (0))) {
          if (((curr) == (k)))
            return pos;
          while (!((curr = key[pos = (pos + 1) & mask]) == (0)))
            if (((curr) == (k))) {
              value[pos] += 1; //SSC: increment in-place
              return pos;
            }
        }
      }
      key[pos] = k;
//      value[pos] = v;
      value[pos] = 1; //SSC: Not found, so we initialize this entry
      if (size++ >= maxFill)
        rehash(arraySize(size + 1, f));
//      if (ASSERTS)  //SSC not necessary, remove
//        checkTable(); //SSC not necessary, remove
      return -1;
    }
  }

}
