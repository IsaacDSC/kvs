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

type bulkDbMock struct {
	calls    int
	gotTable string
	gotItems dto.Items
	err      error
}

func (m *bulkDbMock) BulkSet(_ context.Context, tableName string, its dto.Items) error {
	m.calls++
	m.gotTable = tableName
	m.gotItems = its
	return m.err
}

type replicateNodesMock struct {
	permittedErr    *dto.ErrProposeCmd
	proposeErr      *dto.ErrProposeCmd
	proposeCalls    int
	proposed        commands.Data
	fullClusterAcks int
}

func (m *replicateNodesMock) ProposeCommand(c commands.Data) *dto.ErrProposeCmd {
	m.proposeCalls++
	m.proposed = c
	return m.proposeErr
}

func (m *replicateNodesMock) PermittedProposeCmd() *dto.ErrProposeCmd { return m.permittedErr }

func (m *replicateNodesMock) FullClusterReplicationMinAcks() int { return m.fullClusterAcks }

// doBulkRequest drives the handler through a ServeMux so PathValue("tableName") is populated.
func doBulkRequest(database BulkPutDb, rep ReplicateNodes, table, body string) *httptest.ResponseRecorder {
	h := BulkPutHandle(database, rep)
	mux := http.NewServeMux()
	mux.HandleFunc(h.Pattern, www.HandlerHttp(h.Fn))

	req := httptest.NewRequest(http.MethodPut, "/table/"+table+"/operation/bulk", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// ─── tests ───────────────────────────────────────────────────────────────────

// Happy path: the leader applies the batch locally, then proposes a single BulkPutCmd
// log entry carrying every item and the resolved MinAcks quorum.
func TestBulkPutHandle_appliesAndProposesBatch(t *testing.T) {
	database := &bulkDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 3}

	body := `[{"key":"k1","value":{"x":1}},{"key":"k2","sk":"s","value":{"y":2}}]`
	rec := doBulkRequest(database, rep, "t", body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body=%s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if database.calls != 1 {
		t.Fatalf("BulkSet calls = %d, want 1", database.calls)
	}
	if database.gotTable != "t" {
		t.Fatalf("BulkSet table = %q, want %q", database.gotTable, "t")
	}
	if len(database.gotItems) != 2 {
		t.Fatalf("BulkSet items = %d, want 2", len(database.gotItems))
	}

	if rep.proposeCalls != 1 {
		t.Fatalf("ProposeCommand calls = %d, want 1", rep.proposeCalls)
	}
	if rep.proposed.Cmd != commands.BulkPutCmd {
		t.Fatalf("proposed cmd = %q, want %q", rep.proposed.Cmd, commands.BulkPutCmd)
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
func TestBulkPutHandle_honorsExplicitMinAcks(t *testing.T) {
	database := &bulkDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 3}

	h := BulkPutHandle(database, rep)
	mux := http.NewServeMux()
	mux.HandleFunc(h.Pattern, www.HandlerHttp(h.Fn))

	body := `[{"key":"k1","value":{"x":1}}]`
	req := httptest.NewRequest(http.MethodPut, "/table/t/operation/bulk?raft_min_acks=2", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rep.proposed.MinAcks != 2 {
		t.Fatalf("proposed MinAcks = %d, want 2", rep.proposed.MinAcks)
	}
}

func TestBulkPutHandle_rejectsInvalidJSON(t *testing.T) {
	database := &bulkDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 1}

	rec := doBulkRequest(database, rep, "t", `{not valid`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if database.calls != 0 {
		t.Fatalf("BulkSet must not run on bad JSON, got %d calls", database.calls)
	}
	if rep.proposeCalls != 0 {
		t.Fatalf("ProposeCommand must not run on bad JSON, got %d calls", rep.proposeCalls)
	}
}

func TestBulkPutHandle_rejectsInvalidItems(t *testing.T) {
	database := &bulkDbMock{}
	rep := &replicateNodesMock{fullClusterAcks: 1}

	// second item has empty key — Items.Validate must fail.
	body := `[{"key":"k1","value":{"x":1}},{"key":"","value":{"y":2}}]`
	rec := doBulkRequest(database, rep, "t", body)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if database.calls != 0 {
		t.Fatalf("BulkSet must not run on invalid items, got %d calls", database.calls)
	}
	if rep.proposeCalls != 0 {
		t.Fatalf("ProposeCommand must not run on invalid items, got %d calls", rep.proposeCalls)
	}
}

// A non-leader node rejects before touching storage or proposing.
func TestBulkPutHandle_rejectsWhenNotLeader(t *testing.T) {
	database := &bulkDbMock{}
	rep := &replicateNodesMock{
		fullClusterAcks: 3,
		permittedErr:    dto.NewErrProposeCmd(db.ErrFollowerRejectCmd, "follower", "node-2"),
	}

	body := `[{"key":"k1","value":{"x":1}}]`
	rec := doBulkRequest(database, rep, "t", body)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if database.calls != 0 {
		t.Fatalf("BulkSet must not run when not leader, got %d calls", database.calls)
	}
	if rep.proposeCalls != 0 {
		t.Fatalf("ProposeCommand must not run when not leader, got %d calls", rep.proposeCalls)
	}
}

// A local apply failure aborts the request before proposing to Raft.
func TestBulkPutHandle_localApplyErrorSkipsPropose(t *testing.T) {
	database := &bulkDbMock{err: db.ErrTableNotFound}
	rep := &replicateNodesMock{fullClusterAcks: 3}

	body := `[{"key":"k1","value":{"x":1}}]`
	rec := doBulkRequest(database, rep, "t", body)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if rep.proposeCalls != 0 {
		t.Fatalf("ProposeCommand must not run after local apply failure, got %d calls", rep.proposeCalls)
	}
}

// A failed proposal surfaces as an error status even though the leader already applied locally.
func TestBulkPutHandle_proposeErrorSurfaces(t *testing.T) {
	database := &bulkDbMock{}
	rep := &replicateNodesMock{
		fullClusterAcks: 3,
		proposeErr:      dto.NewErrProposeCmd(errors.New("replication timeout"), "leader", "node-1"),
	}

	body := `[{"key":"k1","value":{"x":1}}]`
	rec := doBulkRequest(database, rep, "t", body)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if database.calls != 1 {
		t.Fatalf("BulkSet should have applied locally before propose, got %d calls", database.calls)
	}
}
