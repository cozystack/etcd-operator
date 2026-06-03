package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Blocker #1: a setup failure must propagate out of the command (RunE) so
// rootCmd.Execute() can translate it into a non-zero exit code, rather than the
// old Run that printed the error and let the process exit 0. PodName is unset,
// so setupEtcdClient fails before touching any cluster — no kubeconfig needed.
func TestStatusCmd_PropagatesSetupError(t *testing.T) {
	cmd := createStatusCmd(&Config{}) // PodName == "" → setup fails fast
	if cmd.RunE == nil {
		t.Fatal("status command must use RunE so failures set a non-zero exit code")
	}
	if err := cmd.RunE(cmd, nil); err == nil {
		t.Fatal("expected an error when the pod name is unset; got nil (would exit 0)")
	}
}

// Every leaf subcommand must be RunE, not Run — a leaf using Run silently
// swallows setup/operation errors and exits 0.
func TestLeafCommandsUseRunE(t *testing.T) {
	config := &Config{}
	leaves := []*cobra.Command{
		createStatusCmd(config),
		createDefragCmd(config),
		createCompactCmd(config),
		createForfeitLeadershipCmd(config),
		createLeaveCmd(config),
		createMembersCmd(config),
		createRemoveMemberCmd(config),
		createAddMemberCmd(config),
		createSnapshotCmd(config),
	}
	// `alarm` is a parent command; its leaves are list/disarm.
	leaves = append(leaves, createAlarmCmd(config).Commands()...)

	for _, c := range leaves {
		if c.RunE == nil {
			t.Errorf("command %q must use RunE (Run swallows errors and exits 0)", c.Name())
		}
	}
}

// Blocker #2: DbSize 0 (freshly initialized member) must not render "NaN%".
func TestInUsePercent_ZeroDbSize(t *testing.T) {
	got := inUsePercent(0, 0)
	if got != "0.00%" {
		t.Errorf("inUsePercent(0,0) = %q, want %q (must not be NaN%%)", got, "0.00%")
	}
	if strings.Contains(got, "NaN") {
		t.Error("inUsePercent must never produce NaN")
	}
}

func TestInUsePercent_Normal(t *testing.T) {
	if got := inUsePercent(200, 50); got != "25.00%" {
		t.Errorf("inUsePercent(200,50) = %q, want %q", got, "25.00%")
	}
}

// Blocker #3: the ERRORS column advertised in the header must actually be
// populated by statusRow. Also guards #2 end-to-end (no NaN in the row).
func TestStatusRow_IncludesErrorsAndNoNaN(t *testing.T) {
	status := &clientv3.StatusResponse{
		Header:      &pb.ResponseHeader{MemberId: 0xabc},
		DbSize:      0, // exercise the divide-by-zero guard within the row
		DbSizeInUse: 0,
		Errors:      []string{"NOSPACE", "CORRUPT"},
	}

	row := statusRow(status)

	if strings.Contains(row, "NaN") {
		t.Errorf("statusRow produced NaN: %q", row)
	}
	for _, want := range []string{"NOSPACE", "CORRUPT"} {
		if !strings.Contains(row, want) {
			t.Errorf("statusRow %q is missing reported error %q", row, want)
		}
	}
	if !strings.Contains(statusHeader(), "ERRORS") {
		t.Fatal("header lost its ERRORS column")
	}
}
