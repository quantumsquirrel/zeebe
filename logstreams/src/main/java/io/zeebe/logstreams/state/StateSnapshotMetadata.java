/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.logstreams.state;

import java.util.Objects;

public class StateSnapshotMetadata implements Comparable<StateSnapshotMetadata> {
  public static final long INITIAL_LAST_WRITTEN_EVENT_POSITION = -1;

  private final long lastWrittenEventPosition;
  private final int lastWrittenEventTerm;
  private final boolean exists;

  /**
   * @param term the current term
   * @return a snapshot metadata based on nothing but the term
   */
  public static StateSnapshotMetadata createInitial(int term) {
    return new StateSnapshotMetadata(INITIAL_LAST_WRITTEN_EVENT_POSITION, term, false);
  }

  public StateSnapshotMetadata(long lastWrittenEventPosition) {
    this(lastWrittenEventPosition, 0, true);
  }

  public StateSnapshotMetadata(
      long lastWrittenEventPosition, int lastWrittenEventTerm, boolean exists) {
    this.lastWrittenEventPosition = lastWrittenEventPosition;
    this.lastWrittenEventTerm = lastWrittenEventTerm;
    this.exists = exists;
  }

  /**
   * Returns the last written event position when the state was last updated.
   *
   * @return last written event position
   */
  public long getLastWrittenEventPosition() {
    return lastWrittenEventPosition;
  }

  /**
   * Returns the term of the last written event
   *
   * @return term of the last written event
   */
  public int getLastWrittenEventTerm() {
    return lastWrittenEventTerm;
  }

  /** @return true if there is a state associated to it, false otherwise */
  public boolean exists() {
    return exists;
  }

  public boolean isInitial() {
    return getLastWrittenEventPosition() == INITIAL_LAST_WRITTEN_EVENT_POSITION;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StateSnapshotMetadata)) {
      return false;
    }

    final StateSnapshotMetadata other = (StateSnapshotMetadata) obj;

    return getLastWrittenEventPosition() == other.getLastWrittenEventPosition()
        && getLastWrittenEventTerm() == other.getLastWrittenEventTerm();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getLastWrittenEventPosition(), getLastWrittenEventTerm(), exists());
  }

  @Override
  public String toString() {
    return "StateSnapshotMetadata{"
        + "lastWrittenEventPosition="
        + getLastWrittenEventPosition()
        + ", lastWrittenEventTerm="
        + getLastWrittenEventTerm()
        + ", exists="
        + exists()
        + '}';
  }

  @Override
  public int compareTo(StateSnapshotMetadata o) {
    int result = Integer.compare(getLastWrittenEventTerm(), o.getLastWrittenEventTerm());
    if (result == 0) {
      result = Long.compare(getLastWrittenEventPosition(), o.getLastWrittenEventPosition());
    }

    return result;
  }
}
