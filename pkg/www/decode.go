package www

import (
	"fmt"
	"net/http"
	"reflect"
)

// decodeParams preenche um struct apontado por out usando tags:
//   - param:"nome" → r.PathValue("nome")
//   - query:"nome" → r.URL.Query().Get("nome")
//
// Campos sem valor na fonte ficam em zero-value. Tipos com Kind string (incl. aliases como operationType) são aceites.
func DecodeParams(r *http.Request, out any) error {
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("decodeParams: precisa de ponteiro não-nulo")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("decodeParams: destino deve ser struct")
	}

	rt := rv.Type()
	q := r.URL.Query()

	for i := 0; i < rv.NumField(); i++ {
		ft := rt.Field(i)
		fv := rv.Field(i)
		if !fv.CanSet() {
			continue
		}

		var raw string
		switch {
		case ft.Tag.Get("param") != "":
			raw = r.PathValue(ft.Tag.Get("param"))
		case ft.Tag.Get("query") != "":
			raw = q.Get(ft.Tag.Get("query"))
		default:
			continue
		}

		if raw == "" {
			continue
		}

		if err := assignPathOrQueryField(fv, raw); err != nil {
			return fmt.Errorf("%s: %w", ft.Name, err)
		}
	}
	return nil
}

func assignPathOrQueryField(f reflect.Value, s string) error {
	switch f.Kind() {
	case reflect.String:
		f.SetString(s)
		return nil
	default:
		return fmt.Errorf("tipo não suportado: %v", f.Kind())
	}
}
