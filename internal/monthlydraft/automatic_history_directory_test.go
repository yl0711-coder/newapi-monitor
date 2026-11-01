package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestAutomaticHistoryDirectoryNewOrganizationsAndSameLabelsRemainSeparate(t *testing.T) {
	db, ro, path := usageAuthorityFixture(t)
	for i := range 5 {
		gid := int64(4100 + i)
		if err := db.Create(&monitor.CustomerGroup{ID: gid, Name: fmt.Sprintf("org-%d", i), PortalEmail: fmt.Sprintf("org-%d@example.invalid", i), PortalAuthVer: 1}).Error; err != nil {
			t.Fatal(err)
		}
		for j := range i + 1 {
			if err := db.Create(&monitor.TrackedUser{UserID: gid*100 + int64(j), GroupID: gid, Username: "same-label", Email: "private@example.invalid", Note: "private-note"}).Error; err != nil {
				t.Fatal(err)
			}
		}
		owner, ids, err := LoadUsageGenerationOwner(t.Context(), ro, gid, 1)
		if err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := captureAutomaticHistoryDirectory(t.Context(), ro, owner)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeAutomaticHistoryDirectory(raw, owner)
		if err != nil || len(got.Members) != len(ids) {
			t.Fatal("directory incomplete", err)
		}
		for k, m := range got.Members {
			if m.UserID != ids[k] || m.Username != "same-label" {
				t.Fatal("labels merged identities", got)
			}
		}
		for _, secret := range []string{"private@example.invalid", "private-note", "portal_email", "portal_pw", "balance_quota"} {
			if bytes.Contains(raw, []byte(secret)) {
				t.Fatal("directory leaked unrelated fields", secret)
			}
		}
		again, err := captureAutomaticHistoryDirectory(t.Context(), ro, owner)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatal("same labels changed frozen bytes", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("read path wrote authority DB", err)
		}
	}
}

func TestAutomaticHistoryDirectoryRejectsRosterChangesAndMissingNames(t *testing.T) {
	db, ro, _ := usageAuthorityFixture(t)
	if err := db.Model(&monitor.TrackedUser{}).Where("group_id=?", 1).Update("username", "same-label").Error; err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(t.Context(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := captureAutomaticHistoryDirectory(t.Context(), ro, owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&monitor.TrackedUser{}).Where("user_id=?", 201).Update("username", "").Error; err != nil {
		t.Fatal(err)
	}
	if got, err := captureAutomaticHistoryDirectory(t.Context(), ro, owner); err == nil || len(got) != 0 {
		t.Fatal("missing label guessed from ID")
	}
	if err := db.Model(&monitor.TrackedUser{}).Where("user_id=?", 201).Update("username", "renamed").Error; err != nil {
		t.Fatal(err)
	}
	renamed, err := captureAutomaticHistoryDirectory(t.Context(), ro, owner)
	if err != nil || bytes.Equal(raw, renamed) {
		t.Fatal("rename not retained", err)
	}
	old, err := decodeAutomaticHistoryDirectory(raw, owner)
	if err != nil || old.Members[0].Username != "same-label" {
		t.Fatal("old label snapshot rewritten", err)
	}
	if err := db.Create(&monitor.TrackedUser{UserID: 203, GroupID: 1, Username: "new-member"}).Error; err != nil {
		t.Fatal(err)
	}
	if got, err := captureAutomaticHistoryDirectory(t.Context(), ro, owner); !errors.Is(err, ErrGenerationChanged) || len(got) != 0 {
		t.Fatal("old owner used after new enrollment", err)
	}
}

func TestAutomaticHistoryDirectoryStrictBytesAndCancellation(t *testing.T) {
	db, ro, _ := usageAuthorityFixture(t)
	if err := db.Model(&monitor.TrackedUser{}).Where("group_id=?", 1).Update("username", "label").Error; err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(t.Context(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := captureAutomaticHistoryDirectory(t.Context(), ro, owner)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		append([]byte(" "), raw...),
		bytes.Replace(raw, []byte(`"mode"`), []byte(`"Mode"`), 1),
		bytes.Replace(raw, []byte(`"name":`), []byte(`"unknown":1,"name":`), 1),
		[]byte(`null`),
	} {
		if _, err := decodeAutomaticHistoryDirectory(bad, owner); err == nil {
			t.Fatal("noncanonical directory accepted")
		}
	}
	d, err := decodeAutomaticHistoryDirectory(raw, owner)
	if err != nil {
		t.Fatal(err)
	}
	d.Members[1] = d.Members[0]
	if _, err := decodeAutomaticHistoryDirectory(materialPreparationJSON(t, d), owner); err == nil {
		t.Fatal("duplicate ID accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := captureAutomaticHistoryDirectory(ctx, ro, owner); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	foreign := owner
	foreign.GroupID = 2
	if _, err := decodeAutomaticHistoryDirectory(raw, foreign); err == nil {
		t.Fatal("cross-org directory accepted")
	}
	var shape map[string]json.RawMessage
	if json.Unmarshal(raw, &shape) != nil || len(shape) != 5 || !strings.Contains(string(raw), automaticHistoryDirectoryMode) {
		t.Fatal("unexpected metadata shape")
	}
	if !reflect.DeepEqual(owner, d.Owner) {
		t.Fatal("owner changed")
	}
}
