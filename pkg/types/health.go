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

package types

import (
	"fmt"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/wrappers"
	flag "github.com/spf13/pflag"
)

const (
	// DefaultSnapshotLeaseRenewalEnabled is a default value for enabling the snapshot lease renewal feature
	DefaultSnapshotLeaseRenewalEnabled = false
	// DefaultMemberLeaseRenewalEnabled is a default value for enabling the member lease renewal feature
	DefaultMemberLeaseRenewalEnabled = false
	// DefaultMemberGCEnabled is a default value for enabling the etcd member garbage collection feature
	DefaultMemberGCEnabled = false
	// DefaultFullSnapshotLeaseName is the name for the delta snapshot lease.
	DefaultFullSnapshotLeaseName = "full-snapshot-revisions"
	// DefaultDeltaSnapshotLeaseName is the name for the delta snapshot lease.
	DefaultDeltaSnapshotLeaseName = "delta-snapshot-revisions"
	// DefaultHeartbeatDuration is the default heartbeat duration or lease renewal deletion.
	DefaultHeartbeatDuration = 30 * time.Second
	// LeaseUpdateTimeoutDuration is the timeout duration for updating snapshot leases
	LeaseUpdateTimeoutDuration = 60 * time.Second
	// DefaultMemberGarbageCollectionPeriod is the default member garbage collection period.
	DefaultMemberGarbageCollectionPeriod = 60 * time.Second
)

// HealthConfig holds the health configuration.
type HealthConfig struct {
	SnapshotLeaseRenewalEnabled bool              `json:"snapshotLeaseRenewalEnabled,omitempty"`
	MemberLeaseRenewalEnabled   bool              `json:"memberLeaseRenewalEnabled,omitempty"`
	MemberGCEnabled             bool              `json:"memberGCEnabled,omitempty"`
	HeartbeatDuration           wrappers.Duration `json:"heartbeatDuration,omitempty"`
	MemberGCDuration            wrappers.Duration `json:"memberGCDuration,omitempty"`
	FullSnapshotLeaseName       string            `json:"fullSnapshotLeaseName,omitempty"`
	DeltaSnapshotLeaseName      string            `json:"deltaSnapshotLeaseName,omitempty"`
}

// NewHealthConfig returns the health config.
func NewHealthConfig() *HealthConfig {
	return &HealthConfig{
		SnapshotLeaseRenewalEnabled: DefaultSnapshotLeaseRenewalEnabled,
		MemberLeaseRenewalEnabled:   DefaultMemberLeaseRenewalEnabled,
		MemberGCEnabled:             DefaultMemberGCEnabled,
		HeartbeatDuration:           wrappers.Duration{Duration: DefaultHeartbeatDuration},
		MemberGCDuration:            wrappers.Duration{Duration: DefaultMemberGarbageCollectionPeriod},
		FullSnapshotLeaseName:       DefaultFullSnapshotLeaseName,
		DeltaSnapshotLeaseName:      DefaultDeltaSnapshotLeaseName,
	}
}

// AddFlags adds the flags to flagset.
func (c *HealthConfig) AddFlags(fs *flag.FlagSet) {

	fs.BoolVar(&c.SnapshotLeaseRenewalEnabled, "enable-snapshot-lease-renewal", c.SnapshotLeaseRenewalEnabled, "Allows sidecar to renew the snapshot leases when snapshots are taken")
	fs.BoolVar(&c.MemberLeaseRenewalEnabled, "enable-member-lease-renewal", c.MemberLeaseRenewalEnabled, "Allows sidecar to periodically renew the member leases")
	fs.BoolVar(&c.MemberGCEnabled, "enable-etcd-member-gc", c.MemberGCEnabled, "Allows leading sidecar to remove any superfluous etcd members from the cluster")
	fs.DurationVar(&c.HeartbeatDuration.Duration, "k8s-heartbeat-duration", c.HeartbeatDuration.Duration, "Heartbeat duration")
	fs.DurationVar(&c.MemberGCDuration.Duration, "k8s-member-gc-duration", c.MemberGCDuration.Duration, "Etcd member garbage collection duration")
	fs.StringVar(&c.FullSnapshotLeaseName, "full-snapshot-lease-name", c.FullSnapshotLeaseName, "full snapshot lease name")
	fs.StringVar(&c.DeltaSnapshotLeaseName, "delta-snapshot-lease-name", c.DeltaSnapshotLeaseName, "delta snapshot lease name")
}

// Validate validates the health Config.
func (c *HealthConfig) Validate() error {
	if c.HeartbeatDuration.Seconds() <= 0 {
		return fmt.Errorf("heartbeat period should be greater than zero")

	}

	if c.MemberGCDuration.Seconds() <= 0 {
		return fmt.Errorf("etcd member garbage collection period should be greater than zero")

	}

	if c.SnapshotLeaseRenewalEnabled {
		if len(c.FullSnapshotLeaseName) == 0 {
			return fmt.Errorf("FullSnapshotLeaseName can not be an empty string when enable-snapshot-lease-renewal is true")
		}
		if len(c.DeltaSnapshotLeaseName) == 0 {
			return fmt.Errorf("DeltaSnapshotLeaseName can not be an empty string when enable-snapshot-lease-renewal is true")
		}
	}
	return nil

}
