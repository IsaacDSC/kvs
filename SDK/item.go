package sdk

import "encoding/json"

type Item struct {
	Key     string         `json:"key"`
	SK      string         `json:"sk"`
	Value   map[string]any `json:"value"`
	Version string         `json:"version,omitempty"`
}

type inputItem struct {
	key     string
	sk      string
	value   any
	version *inputVersion
}

func (i *inputItem) Json() []byte {
	it := map[string]any{
		"key":   i.key,
		"sk":    i.sk,
		"value": i.value,
	}

	if i.version != nil {
		it["version"] = map[string]any{
			"propose_version": i.version.propose,
			"old_version":     i.version.old,
		}
	}

	b, _ := json.Marshal(it)

	return b
}

type inputVersion struct {
	old     string
	propose string
}

func NewItem() *inputItem {
	return &inputItem{}
}

func (i *inputItem) WithKey(key string) *inputItem {
	i.key = key
	return i
}

func (i *inputItem) WithSk(sk string) *inputItem {
	i.sk = sk
	return i
}

func (i *inputItem) WithValue(value any) *inputItem {
	i.value = value
	return i
}

func (i *inputItem) WithVersion(dbV, proposeV string) *inputItem {
	i.version = &inputVersion{
		old:     dbV,
		propose: proposeV,
	}
	return i
}

func (i *inputItem) Build() *inputItem {
	return i
}
