// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package cmd

import (
	"context"
	"os"

	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/spf13/cobra"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

// NewAddMemberCommand Adds a new member to the etcd cluster
func NewAddMemberCommand(ctx context.Context) *cobra.Command {
	opts := newAddmemberOptions()
	// compactCmd represents the restore command
	compactCmd := &cobra.Command{
		Use:   "addmember",
		Short: "test short command. When etcdbrctl --help is called",
		Long:  "Test long command. When etcdbrctl addmember --help is called",
		Run: func(cmd *cobra.Command, args []string) {
			logger.Info("Starting member add routine")

			//Create k8s client set
			clientSet, err := miscellaneous.GetKubernetesClientSetOrError()
			if err != nil {
				logger.Errorf("failed to create clientset: %v", err)
				return
			}

			podName := os.Getenv("POD_NAME")
			namespace := os.Getenv("POD_NAMESPACE")
			stsName := podName[:strings.LastIndex(podName, "-")]

			// Check sts to differentiate b/w cluster scale-up and cluster wake-up
			curSts := &appsv1.StatefulSet{}
			stsErr := clientSet.Get(ctx, client.ObjectKey{
				Namespace: namespace,
				Name:      stsName, // TODO derive this from POD_NAME
			}, curSts)
			if stsErr != nil {
				logger.Warn("error fetching etcd sts: ", stsErr)
			}

			//TODO: Incorporate object generation as part of the check here
			if !(*curSts.Spec.Replicas >= 1 && *curSts.Spec.Replicas > curSts.Status.UpdatedReplicas) {
				logger.Info("All pods already upto date with sts. Returning.... ")
				return
			}

			// Add member as a learner
			// This is needed as adding a new member should not affect the already running cluster
			// A cluster can only have one learner at a time. Adding a 2nd learner returns an error
			for {
				clientFactory := etcdutil.NewFactory(brtypes.EtcdConnectionConfig{
					Endpoints:         []string{os.Getenv("ETCD_ENDPOINT")}, //[]string{"http://etcd-main-peer.default.svc.cluster.local:2380"},
					InsecureTransport: true,
				})
				cli, _ := clientFactory.NewCluster()

				ctx3, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
				etcdList, err2 := cli.MemberList(ctx3)
				if err2 != nil {
					logger.Warn("Could not list any etcd members")
					continue
				}
				cancel()
				mem := make(map[string]bool)
				for _, y := range etcdList.Members {
					mem[y.Name] = true
				}
				if mem[podName] {
					break
				}

				ctx2, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
				defer cancel()
				memberURL := getMemberURL()
				if memberURL == "" {
					continue
				}
				_, err2 = cli.MemberAddAsLearner(ctx2, []string{memberURL})
				if err2 != nil {
					logger.Warn("Error adding member as a learner: ", err2)
				}
				//Break loop if member successfully added as a learner or if URL was already present in the cluster (can happen in cases when container has to restart)
				if err2 == nil || strings.Contains(rpctypes.ErrGRPCPeerURLExist.Error(), err2.Error()) {
					break
					//TODO: why not just return here?
				}
				cli.Close()

				timer := time.NewTimer(5 * time.Second)
				<-timer.C
				timer.Stop()

			}

			logger.Info("Member added to the cluster")

		},
	}

	opts.addFlags(compactCmd.Flags())
	return compactCmd
}
