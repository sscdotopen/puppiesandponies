package puppiesandponies.analysis;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class HistoryMatrix {

  private IntArrayList rows[];

  public HistoryMatrix(int numUsers, int initialCapacityPerUser) {
    this.rows = new IntArrayList[numUsers];
    for (int n = 0; n < numUsers; n++) {
      rows[n] = new IntArrayList(initialCapacityPerUser);
    }
  }

  public int numUsers() {
    return rows.length;
  }

  public IntArrayList historyOfUser(int user) {
    return rows[user];
  }

  public void observeInteraction(int user, int item) {
    rows[user].add(item);
  }
}
