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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.logstreams.util.RocksDBWrapper;
import io.zeebe.test.util.AutoCloseableRule;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

public class StateSnapshotControllerTest {
  @Rule public TemporaryFolder tempFolderRule = new TemporaryFolder();
  @Rule public AutoCloseableRule autoCloseableRule = new AutoCloseableRule();

  private StateSnapshotController snapshotController;
  private StateStorage storage;

  @Before
  public void setup() throws IOException {
    final File snapshotsDirectory = tempFolderRule.newFolder("snapshots");
    final File runtimeDirectory = tempFolderRule.newFolder("runtime");
    storage = new StateStorage(runtimeDirectory, snapshotsDirectory);

    snapshotController =
        new StateSnapshotController(
            ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class), storage);

    autoCloseableRule.manage(snapshotController);
  }

  @Test
  public void shouldThrowExceptionOnTakeSnapshotIfClosed() throws Exception {
    // given

    // then
    assertThat(snapshotController.isDbOpened()).isFalse();
    assertThatThrownBy(() -> snapshotController.takeSnapshot(1))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void shouldTakeSnapshot() throws Exception {
    // given
    final String key = "test";
    final int value = 3;
    final RocksDBWrapper wrapper = new RocksDBWrapper();

    // when
    wrapper.wrap(snapshotController.openDb());
    wrapper.putInt(key, value);
    snapshotController.takeSnapshot(1);
    snapshotController.close();
    wrapper.wrap(snapshotController.openDb());

    // then
    assertThat(wrapper.getInt(key)).isEqualTo(value);
  }

  @Test
  public void shouldOpenNewDatabaseIfNoSnapshotsToRecoverFrom() throws Exception {
    // given

    // when
    final long lowerBoundSnapshotPosition = snapshotController.recover();

    // then
    assertThat(lowerBoundSnapshotPosition).isEqualTo(-1);
  }

  @Test
  public void shouldRemovePreExistingNewDatabaseOnRecoverIfNoSnapshotsToRecoverFrom()
      throws Exception {
    // given
    final String key = "test";
    final int value = 1;
    final RocksDBWrapper wrapper = new RocksDBWrapper();

    // when
    wrapper.wrap(snapshotController.openDb());
    wrapper.putInt(key, value);
    snapshotController.close();
    final long lowerBound = snapshotController.recover();
    wrapper.wrap(snapshotController.openDb());

    // then
    assertThat(lowerBound).isEqualTo(-1);
    assertThat(wrapper.mayExist(key)).isFalse();
  }

  @Test
  public void shouldReturnLatestLowerBound() throws Exception {
    // given
    snapshotController.openDb();
    snapshotController.takeSnapshot(1);
    snapshotController.takeSnapshot(2);
    snapshotController.takeSnapshot(3);

    // when
    final long latestLowerBound = snapshotController.recover();

    // then
    assertThat(latestLowerBound).isEqualTo(3);
  }

  @Test
  public void shouldEnsureMaxSnapshotCount() throws Exception {
    // given
    snapshotController.openDb();
    snapshotController.takeSnapshot(16);
    snapshotController.takeSnapshot(2322);
    snapshotController.takeSnapshot(131);
    snapshotController.takeSnapshot(45);
    snapshotController.takeSnapshot(34);

    // when
    snapshotController.ensureMaxSnapshotCount(2);

    // then
    assertThat(storage.list()).hasSize(2);
    assertThat(storage.list()).extracting(f -> f.getName()).containsOnly("2322", "131");
    final long latestLowerBound = snapshotController.recover();
    assertThat(latestLowerBound).isEqualTo(2322);
  }

  @Test
  public void shouldRecoverFromLatestNotCorruptedSnapshot() throws Exception {
    // given two snapshots
    final RocksDBWrapper wrapper = new RocksDBWrapper();
    wrapper.wrap(snapshotController.openDb());

    wrapper.putInt("x", 1);
    snapshotController.takeSnapshot(1);

    wrapper.putInt("x", 2);
    snapshotController.takeSnapshot(2);

    snapshotController.close();
    corruptSnapshot(2);

    // when
    final long lowerBound = snapshotController.recover();
    wrapper.wrap(snapshotController.openDb());

    // then
    assertThat(lowerBound).isEqualTo(1);
    assertThat(wrapper.getInt("x")).isEqualTo(1);
  }

  @Test
  public void shouldFailToOpenDatabaseIfAllSnapshotsAreCorrupted() throws Exception {
    // given two snapshots
    final RocksDBWrapper wrapper = new RocksDBWrapper();

    wrapper.wrap(snapshotController.openDb());
    wrapper.putInt("x", 1);

    snapshotController.takeSnapshot(1);
    snapshotController.close();
    corruptSnapshot(1);

    // when/then
    assertThatThrownBy(() -> snapshotController.recover())
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Failed to recover snapshot")
        .hasRootCauseInstanceOf(RocksDBException.class);
  }

  private void corruptSnapshot(long position) throws IOException {
    final File snapshot = storage.getSnapshotDirectoryFor(position);
    assertThat(snapshot).isNotNull();

    final File[] files = snapshot.listFiles((dir, name) -> name.endsWith(".sst"));
    assertThat(files).hasSizeGreaterThan(0);

    final File file = files[0];
    Files.write(file.toPath(), "<--corrupted-->".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
  }
}
