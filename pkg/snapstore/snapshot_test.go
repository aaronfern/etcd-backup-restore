// Copyright © 2018 The Gardener Authors.
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

package snapstore_test

import (
	"fmt"
	"sort"
	"time"

	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snapshot", func() {

	Describe("Snapshot service", func() {
		Context("when provied with list of snapshot", func() {
			It("sorts snapshot by creation time", func() {
				interval := int64(5)
				now := time.Now().Unix()
				snap1 := Snapshot{
					CreatedOn:     time.Unix(now, 0),
					StartRevision: 0,
					LastRevision:  2088,
					Kind:          SnapshotKindFull,
				}
				snap2 := Snapshot{
					CreatedOn:     time.Unix(now+1*interval, 0),
					StartRevision: 0,
					LastRevision:  1988,
					Kind:          SnapshotKindFull,
				}
				snap3 := Snapshot{
					CreatedOn:     time.Unix(now+2*interval, 0),
					StartRevision: 0,
					LastRevision:  1888,
					Kind:          SnapshotKindFull,
				}
				snap4 := Snapshot{
					CreatedOn:     time.Unix(now+3*interval, 0),
					StartRevision: 0,
					LastRevision:  1788,
					Kind:          SnapshotKindFull,
				}
				snap5 := Snapshot{
					CreatedOn:     time.Unix(now+4*interval, 0),
					StartRevision: 0,
					LastRevision:  1688,
					Kind:          SnapshotKindFull,
				}
				snap6 := Snapshot{
					CreatedOn:     time.Unix(now+5*interval, 0),
					StartRevision: 0,
					LastRevision:  1588,
					Kind:          SnapshotKindFull,
				}
				snapList := SnapList{&snap4, &snap3, &snap1, &snap6, &snap2, &snap5}
				sort.Sort(snapList)
				for i := 0; i < len(snapList); i++ {
					Expect(snapList[i].CreatedOn.Unix()).To(Equal(now + int64(i)*interval))
				}
			})
		})

		Context("given a snapshot", func() {
			now := time.Now().Unix()
			snap1 := Snapshot{
				CreatedOn:     time.Unix(now, 0),
				StartRevision: 0,
				LastRevision:  2088,
				Kind:          SnapshotKindFull,
			}
			It("generates snapshot name ", func() {
				snap1.GenerateSnapshotName()
				Expect(snap1.SnapName).Should(Equal(fmt.Sprintf("Full-00000000-00002088-%08d", now)))
			})
			It("generates snapshot directory name ", func() {
				snap1.GenerateSnapshotDirectory()
				Expect(snap1.SnapDir).Should(Equal(fmt.Sprintf("Backup-%08d", now)))
			})
		})
	})

	Describe("Parse Snapshot name", func() {
		Context("when valid snapshot name provided", func() {
			It("does not return error", func() {
				snapName := "Backup-1518427675/Full-00000000-00030009-1518427675"
				_, err := ParseSnapshot(snapName)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("when number of separated tokens not equal to 4", func() {
			It("returns error", func() {
				snapName := "Backup-2387428/Full-00000000-00002088-2387428-43"
				_, err := ParseSnapshot(snapName)
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("when non integer start revision specified", func() {
			It("returns error", func() {
				snapName := "Backup-2387428/Full-00h000000-00002088-2387428"
				_, err := ParseSnapshot(snapName)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when not integer last revision specified", func() {
			It("returns error", func() {
				snapName := "Backup-2387428/Full-00000000-00sdf002088-2387428"
				_, err := ParseSnapshot(snapName)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when start revision is more than last revision", func() {
			It("returns error", func() {
				snapName := "Backup-2387428/Full-00012345-00002088-2387428"
				_, err := ParseSnapshot(snapName)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when non integer unix time specified", func() {
			It("returns error", func() {
				snapName := "Backup-23874sdf43/Full-00000000-00002088-23874sdf43"
				_, err := ParseSnapshot(snapName)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("when invalid kind is specified", func() {
			It("returns error", func() {
				snapName := "Backup-2387428/meta-00000000-00002088-2387428"
				_, err := ParseSnapshot(snapName)
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
