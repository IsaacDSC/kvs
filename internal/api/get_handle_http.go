package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type GetDb interface {
	Get(ctx context.Context, tableName string, key string) (item.Entity, error)
}

type getParams struct {
	TableName string `param:"tableName"`
	Key       string `param:"key"`
}

func GetHandler(db GetDb) www.Handler {
	return www.Handler{
		Pattern: "GET /table/{tableName}/{key}",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var params getParams
			if err := www.DecodeParams(r, &params); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			entity, err := db.Get(r.Context(), params.TableName, params.Key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// entity.Value may be map[interface{}]interface{} after CBOR (DecodeItem); encoding/json rejects that.
			resp := struct {
				TableName string `json:"table_name"`
				Key       string `json:"key"`
				SK        string `json:"sk,omitempty"`
				Value     any    `json:"value"`
				Version   string `json:"version,omitempty"`
			}{
				TableName: params.TableName,
				Key:       entity.Key,
				SK:        entity.SK,
				Value:     jsonSafeAny(entity.Value),
				Version:   entity.Version,
			}
			www.WriteJSON(w, http.StatusOK, resp)
		},
	}
}

type getBySecondaryKeyDb interface {
	GetBySecondaryKey(ctx context.Context, tableName string, secondaryKey string) ([]item.Entity, error)
}

type getBySecondaryKeyParams struct {
	TableName    string `param:"tableName"`
	SecondaryKey string `query:"sk"`
}

func GetBySecondaryKeyHandler(db getBySecondaryKeyDb) www.Handler {
	return www.Handler{
		Pattern: "GET /table/{tableName}",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var params getBySecondaryKeyParams
			if err := www.DecodeParams(r, &params); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			entities, err := db.GetBySecondaryKey(r.Context(), params.TableName, params.SecondaryKey)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			type entityResponse struct {
				TableName string `json:"table_name"`
				Key       string `json:"key"`
				SK        string `json:"sk,omitempty"`
				Value     any    `json:"value"`
				Version   string `json:"version,omitempty"`
			}

			out := make([]entityResponse, 0, len(entities))
			for _, e := range entities {
				out = append(out, entityResponse{
					TableName: params.TableName,
					Key:       e.Key,
					SK:        e.SK,
					Value:     jsonSafeAny(e.Value),
					Version:   e.Version,
				})
			}

			www.WriteJSON(w, http.StatusOK, out)
		},
	}
}

// jsonSafeAny converts values that encoding/json cannot marshal (e.g. map[interface{}]interface{}
// produced when CBOR-decoding nested maps) into JSON-friendly shapes.
func jsonSafeAny(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, val := range x {
			m[k] = jsonSafeAny(val)
		}
		return m
	case map[interface{}]interface{}:
		m := make(map[string]any, len(x))
		for k, val := range x {
			var ks string
			if s, ok := k.(string); ok {
				ks = s
			} else {
				ks = fmt.Sprint(k)
			}
			m[ks] = jsonSafeAny(val)
		}
		return m
	case []interface{}:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = jsonSafeAny(val)
		}
		return out
	default:
		return v
	}
}
