package xtore

// Query the query to be executed
type Query struct {
	Where    string        `json:"where"`
	Order    []string      `json:"order"`
	Paginate string        `json:"paginate"`
	Args     []interface{} `json:"args"`
}
