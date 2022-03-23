// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package initializer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	v1 "k8s.io/api/coordination/v1"
	controller_runtime_client "sigs.k8s.io/controller-runtime/pkg/client"
)

// Initialize has the following steps:
//   * Check if data directory exists.
//     - If data directory exists
//       * Check for data corruption.
//			- If data directory is in corrupted state, clear the data directory.
//     - If data directory does not exist.
//       * Check if Latest snapshot available.
//		   - Try to perform an Etcd data restoration from the latest snapshot.
//		   - No snapshots are available, start etcd as a fresh installation.
func (e *EtcdInitializer) Initialize(mode validator.Mode, failBelowRevision int64) error {
	start := time.Now()
	dataDirStatus, err := e.Validator.Validate(mode, failBelowRevision)
	if dataDirStatus == validator.DataDirectoryStatusUnknown {
		metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
		return fmt.Errorf("error while initializing: %v", err)
	}

	if dataDirStatus == validator.FailBelowRevisionConsistencyError {
		metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
		return fmt.Errorf("failed to initialize since fail below revision check failed")
	}

	metrics.ValidationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Since(start).Seconds())

	if dataDirStatus != validator.DataDirectoryValid {
		start := time.Now()
		restored, err := e.restoreCorruptData()
		if err != nil {
			metrics.RestorationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededFalse}).Observe(time.Since(start).Seconds())
			return fmt.Errorf("error while restoring corrupt data: %v", err)
		}
		if restored {
			metrics.RestorationDurationSeconds.With(prometheus.Labels{metrics.LabelSucceeded: metrics.ValueSucceededTrue}).Observe(time.Since(start).Seconds())
		}
	}
	return nil
}

//NewInitializer creates an etcd initializer object.
func NewInitializer(options *brtypes.RestoreOptions, snapstoreConfig *brtypes.SnapstoreConfig, logger *logrus.Logger) *EtcdInitializer {
	zapLogger, _ := zap.NewProduction()
	etcdInit := &EtcdInitializer{
		Config: &Config{
			SnapstoreConfig: snapstoreConfig,
			RestoreOptions:  options,
		},
		Validator: &validator.DataValidator{
			Config: &validator.Config{
				DataDir:                options.Config.RestoreDataDir,
				EmbeddedEtcdQuotaBytes: options.Config.EmbeddedEtcdQuotaBytes,
				SnapstoreConfig:        snapstoreConfig,
			},
			Logger:    logger,
			ZapLogger: zapLogger,
		},
		Logger: logger,
	}

	return etcdInit
}

func (r *EtcdInitializer) SingleMemberRestoreCase() bool {
	r.Logger.Info("Starting single member restore case")
	clientSet, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		r.Logger.Errorf("failed to create clientset: %v", err)
		return false
	}
	r.Logger.Info("Single member restore case: k8s clientset created")

	//Fetch lease associated with member
	memberLease := &v1.Lease{}
	err = clientSet.Get(context.TODO(), controller_runtime_client.ObjectKey{
		Namespace: os.Getenv("POD_NAMESPACE"),
		Name:      os.Getenv("POD_NAME"),
	}, memberLease)
	if err != nil {
		r.Logger.Errorf("error fetching lease: %v", err)
	}
	r.Logger.Info("Single member restore case: fetched lease")

	if memberLease.Spec.HolderIdentity != nil {
		r.Logger.Info("Single member restore case: lease holder ID is not nil")
		//Case of lease already existing
		//Assume case of single member restoration

		//Create etcd client
		clientFactory := etcdutil.NewFactory(brtypes.EtcdConnectionConfig{
			Endpoints:         []string{"http://etcd-main-peer.default.svc.cluster.local:2380"}, //TODO: Don't hardcode this value
			InsecureTransport: true,
		})
		cli, _ := clientFactory.NewCluster()
		r.Logger.Info("Single member restore case: created etcd client")

		//remove self from cluster
		ctx3, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
		etcdList, err2 := cli.MemberList(ctx3)
		if err2 != nil {
			r.Logger.Warn("Could not list any etcd members")
		}
		cancel()
		r.Logger.Info("Single member restore case: etcd member list done")
		//mem := make(map[string]uint64)
		var peerURL []string
		for _, y := range etcdList.Members {
			if y.Name == os.Getenv("POD_NAME") {
				peerURL = y.PeerURLs
				cli.MemberRemove(context.TODO(), y.ID)
				r.Logger.Info("Single member restore case: member removed")
				break
			}
		}

		//Add self to the cluster
		ctx4, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
		cli.MemberAdd(ctx4, peerURL)
		cancel()
		r.Logger.Info("Single member restore case: member added")
		return true
	}

	return false
}

// restoreCorruptData attempts to restore a corrupted data directory.
// It returns true only if restoration was successful, and false when
// bootstrapping a new data directory or if restoration failed
func (e *EtcdInitializer) restoreCorruptData() (bool, error) {
	logger := e.Logger
	tempRestoreOptions := *(e.Config.RestoreOptions.DeepCopy())
	dataDir := tempRestoreOptions.Config.RestoreDataDir

	if e.Config.SnapstoreConfig == nil || len(e.Config.SnapstoreConfig.Provider) == 0 {
		logger.Warnf("No snapstore storage provider configured.")
		return e.restoreWithEmptySnapstore()
	}
	store, err := snapstore.GetSnapstore(e.Config.SnapstoreConfig)
	if err != nil {
		err = fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
		return false, err
	}
	logger.Info("Finding latest set of snapshot to recover from...")
	baseSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
	if err != nil {
		logger.Errorf("failed to get latest set of snapshot: %v", err)
		return false, err
	}
	if baseSnap == nil && (deltaSnapList == nil || len(deltaSnapList) == 0) {
		// Snapstore is considered to be the source of truth. Thus, if
		// snapstore exists but is empty, data directory should be cleared.
		logger.Infof("No snapshot found. Will remove the data directory.")
		return e.restoreWithEmptySnapstore()
	}

	if e.SingleMemberRestoreCase() {
		logger.Infof("Trying to restore a single member from the cluster... ")
		return e.restoreWithEmptySnapstore()
	}

	tempRestoreOptions.BaseSnapshot = baseSnap
	tempRestoreOptions.DeltaSnapList = deltaSnapList
	tempRestoreOptions.Config.RestoreDataDir = fmt.Sprintf("%s.%s", tempRestoreOptions.Config.RestoreDataDir, "part")

	if err := e.removeDir(tempRestoreOptions.Config.RestoreDataDir); err != nil {
		return false, fmt.Errorf("failed to delete previous temporary data directory: %v", err)
	}

	rs := restorer.NewRestorer(store, logrus.NewEntry(logger))
	if err := rs.RestoreAndStopEtcd(tempRestoreOptions); err != nil {
		err = fmt.Errorf("failed to restore snapshot: %v", err)
		return false, err
	}

	if err := e.removeContents(dataDir); err != nil {
		return false, fmt.Errorf("failed to remove corrupt contents with restored snapshot: %v", err)
	}
	logger.Infoln("Successfully restored the etcd data directory.")
	return true, nil
}

// restoreWithEmptySnapstore removes the data directory as
// part of restoration process for empty snapstore case.
// It returns true if data directory removal is successful,
// and false if directory removal failed or if directory
// never existed (bootstrap case)
func (e *EtcdInitializer) restoreWithEmptySnapstore() (bool, error) {
	dataDir := e.Config.RestoreOptions.Config.RestoreDataDir
	e.Logger.Infof("Removing directory(%s) since snapstore is empty.", dataDir)

	// If data directory doesn't exist, it means we are bootstrapping
	// a new data directory, so no restoration occurs
	if _, err := os.Stat(dataDir); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	// If data directory already exists, then we remove it.
	// This is considered an act of restoration because we
	// act on the corrupted data directory by removing it
	if err := e.removeDir(dataDir); err != nil {
		return false, err
	}
	return true, nil
}

func (e *EtcdInitializer) removeContents(dataDir string) error {
	if err := e.removeDir(dataDir); err != nil {
		return err
	}

	if err := os.Rename(filepath.Join(fmt.Sprintf("%s.%s", dataDir, "part")), filepath.Join(dataDir)); err != nil {
		return fmt.Errorf("failed to rename temp restore directory %s to data directory %s with err: %v", filepath.Join(fmt.Sprintf("%s.%s", dataDir, "part")), dataDir, err)
	}
	return nil
}

func (e *EtcdInitializer) removeDir(dirname string) error {
	e.Logger.Infof("Removing directory(%s).", dirname)
	if err := os.RemoveAll(filepath.Join(dirname)); err != nil {
		return fmt.Errorf("failed to remove directory %s with err: %v", dirname, err)
	}
	return nil
}
