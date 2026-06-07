package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/pkg/www"
)

// ─── test doubles ────────────────────────────────────────────────────────────

type bulkDelDbMock struct {
	calls    int
	gotTable string
	gotItems dto.DeleteItems
	err      error
}

func (m *bulkDelDbMock) BulkDel(_ context.Context, tableName string, its dto.DeleteItems) error {
	m.calls++
	m.gotTable = tableName
	m.gotItems = its
	return m.err
}

// doBulkDelRequest drives the handler through a ServeMux so PathValue("tableName") is populated.
func doBulkDelRequest(database BulkDelDb, rep ReplicateNodes, table, body string) *httptest.ResponseRecorder {
	h := BulkDelHandle(database, rep)
	mux := http.NewServeMux()
	mux.HandleFunc(h.Pattern, www.HandlerHttp(h.Fn))

	req := httptest.NewRequest(http.MethodDelete, "/table/"+table+"/operation/bulk", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// ─── tests ───────────────────────────────────────────────────────────────────

// Happy path: the leader applies the batch locally, then proposes a single BulkDelCmd
// log entry carrying every key and the resolved MinAcks quorum.
func TestBulkDelHandle_appliesAndProposesBatch(t *testing.T) {
	database := &bulkDelDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 3}

	body := `[{"key":"k1"},{"key":"k2"}]`
	rec := doBulkDelRequest(database, rep, "t", body)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d (body=%s)", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if database.calls != 1 {
		t.Fatalf("BulkDel calls = %d, want 1", database.calls)
	}
	if database.gotTable != "t" {
		t.Fatalf("BulkDel table = %q, want %q", database.gotTable, "t")
	}
	if len(database.gotItems) != 2 {
		t.Fatalf("BulkDel items = %d, want 2", len(database.gotItems))
	}

	if rep.proposeCalls != 1 {
		t.Fatalf("ProposeCommand calls = %d, want 1", rep.proposeCalls)
	}
	if rep.proposed.Cmd != commands.BulkDelCmd {
		t.Fatalf("proposed cmd = %q, want %q", rep.proposed.Cmd, commands.BulkDelCmd)
	}
	if rep.proposed.TableName != "t" {
		t.Fatalf("proposed table = %q, want %q", rep.proposed.TableName, "t")
	}
	if len(rep.proposed.Items) != 2 || rep.proposed.Items[0].Key != "k1" || rep.proposed.Items[1].Key != "k2" {
		t.Fatalf("proposed items = %#v", rep.proposed.Items)
	}
	if rep.proposed.MinAcks != 3 {
		t.Fatalf("proposed MinAcks = %d, want 3 (full-cluster default)", rep.proposed.MinAcks)
	}
}

// An explicit raft_min_acks query value flows through to the proposed command unchanged.
func TestBulkDelHandle_honorsExplicitMinAcks(t *testing.T) {
	database := &bulkDelDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 3}

	h := BulkDelHandle(database, rep)
	mux := http.NewServeMux()
	mux.HandleFunc(h.Pattern, www.HandlerHttp(h.Fn))

	body := `[{"key":"k1"}]`
	req := httptest.NewRequest(http.MethodDelete, "/table/t/operation/bulk?raft_min_acks=2", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if rep.proposed.MinAcks != 2 {
		t.Fatalf("proposed MinAcks = %d, want 2", rep.proposed.MinAcks)
	}
}

func TestBulkDelHandle_rejectsInvalidJSON(t *testing.T) {
	database := &bulkDelDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 1}

	rec := doBulkDelRequest(database, rep, "t", `{not valid`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if database.calls != 0 {
		t.Fatalf("BulkDel must not run on bad JSON, got %d calls", database.calls)
	}
	if rep.proposeCalls != 0 {
		t.Fatalf("ProposeCommand must not run on bad JSON, got %d calls", rep.proposeCalls)
	}
}

func TestBulkDelHandle_rejectsItemWithoutKey(t *testing.T) {
	database := &bulkDelDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 1}

	body := `[{"key":"k1"},{"key":""}]`
	rec := doBulkDelRequest(database, rep, "t", body)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if database.calls != 0 {
		t.Fatalf("BulkDel must not run on invalid items, got %d calls", database.calls)
	}
	if rep.proposeCalls != 0 {
		t.Fatalf("ProposeCommand must not run on invalid items, got %d calls", rep.proposeCalls)
	}
}

func TestBulkDelHandle_rejectsEmptyBatch(t *testing.T) {
	database := &bulkDelDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 1}

	rec := doBulkDelRequest(database, rep, "t", `[]`)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if database.calls != 0 {
		t.Fatalf("BulkDel must not run on empty batch, got %d calls", database.calls)
	}
}

// A non-leader node rejects before touching storage or proposing.
func TestBulkDelHandle_rejectsWhenNotLeader(t *testing.T) {
	database := &bulkDelDbMock{}
	rep := &replicateNodesMock{
		fullClusterAcks: 3,
		permittedErr:    dto.NewErrProposeCmd(db.ErrFollowerRejectCmd, "follower", "node-2"),
	}

	body := `[{"key":"k1"}]`
	rec := doBulkDelRequest(database, rep, "t", body)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if database.calls != 0 {
		t.Fatalf("BulkDel must not run when not leader, got %d calls", database.calls)
	}
	if rep.proposeCalls != 0 {
		t.Fatalf("ProposeCommand must not run when not leader, got %d calls", rep.proposeCalls)
	}
}

// A local apply failure aborts the request before proposing to Raft.
func TestBulkDelHandle_localApplyErrorSkipsPropose(t *testing.T) {
	database := &bulkDelDbMock{err: db.ErrTableNotFound}
	rep := &replicateNodesMock{fullClusterAcks: 3}

	body := `[{"key":"k1"}]`
	rec := doBulkDelRequest(database, rep, "t", body)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if rep.proposeCalls != 0 {
		t.Fatalf("ProposeCommand must not run after local apply failure, got %d calls", rep.proposeCalls)
	}
}

// A failed proposal surfaces as an error status even though the leader already applied locally.
func TestBulkDelHandle_proposeErrorSurfaces(t *testing.T) {
	database := &bulkDelDbMock{}
	rep := &replicateNodesMock{
		fullClusterAcks: 3,
		proposeErr:      dto.NewErrProposeCmd(errors.New("replication timeout"), "leader", "node-1"),
	}

	body := `[{"key":"k1"}]`
	rec := doBulkDelRequest(database, rep, "t", body)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if database.calls != 1 {
		t.Fatalf("BulkDel should have applied locally before propose, got %d calls", database.calls)
	}
}
