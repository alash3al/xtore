package xtore

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/imdario/mergo"

	// the posgres driver
	_ "github.com/lib/pq"
)

const (
	tableSchema = `CREATE TABLE IF NOT EXISTS _store_(
		_serial		SERIAL,
		_uuid		UUID DEFAULT gen_random_uuid() PRIMARY KEY,
		_doc		JSONB,
		_updatedAt 	INTEGER,
		_createdAt 	INTEGER,

		INDEX _serial (_serial),
		INVERTED INDEX _doc (_doc),
		INDEX _updatedAt (_updatedAt),
		INDEX _createdAt (_createdAt)
	)`
)

// Store handles the store configurations
type Store struct {
	db *sql.DB
}

// New creates new store
func New(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(tableSchema); err != nil {
		return nil, err
	}
	return &Store{db}, nil
}

// NewUsing create a new instance using the specified sql.DB
func NewUsing(db *sql.DB) (*Store, error) {
	if _, err := db.Exec(tableSchema); err != nil {
		return nil, err
	}
	return &Store{db}, nil
}

// Insert a new data into the store
func (s Store) Insert(data map[string]interface{}) (*Document, error) {
	doc := &Document{}
	doc.Data = data
	doc.CreatedAt = time.Now().Unix()
	doc.UpdatedAt = doc.CreatedAt

	jsonData, _ := json.Marshal(doc.Data)

	err := s.db.QueryRow(`
		INSERT INTO _store_(_doc, _createdAt, _updatedAt) 
		VALUES ($1, $2, $3) 
		RETURNING _serial, _uuid
	`, jsonData, doc.CreatedAt, doc.UpdatedAt).Scan(&doc.Serial, &doc.UUID)

	return doc, err
}

// Get fetch a document using its uuid
func (s *Store) Get(uuid string) (*Document, error) {
	doc := &Document{}
	jsonEncoded := []byte{}
	err := s.db.
		QueryRow(`SELECT _serial, _uuid, _doc, _createdAt, _updatedAt FROM _store_ WHERE _uuid = $1`, uuid).
		Scan(&doc.Serial, &doc.UUID, &jsonEncoded, &doc.CreatedAt, &doc.UpdatedAt)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(jsonEncoded, &doc.Data)
	return doc, nil
}

// Update updates the specified document
func (s *Store) Update(uuid string, data map[string]interface{}) (*Document, error) {
	doc, err := s.Get(uuid)
	if err != nil {
		return nil, err
	}
	doc.UpdatedAt = time.Now().Unix()
	if err := mergo.MergeWithOverwrite(&doc.Data, data); err != nil {
		return nil, err
	}
	jsonData, _ := json.Marshal(doc.Data)
	_, err = s.db.Exec(
		`UPDATE _store_ SET (_doc, _updatedAt) = ($1, $2) WHERE _uuid = $3`,
		jsonData,
		doc.UpdatedAt,
		uuid,
	)
	return doc, err
}

// Search search the data
func (s *Store) Search(q *Query) (*Result, error) {
	if q == nil {
		q = &Query{}
	}

	_sql := "SELECT _serial, _uuid, _doc, _createdAt, _updatedAt FROM _store_ "
	_sqlCount := "SELECT COUNT(_serial) FROM _store_ "

	if q.Where != "" {
		_sql += " WHERE (" + q.Where + ") "
		_sqlCount += " WHERE (" + q.Where + ") "
	}

	totals := int64(0)
	now := time.Now()

	s.db.QueryRow(_sqlCount).Scan(&totals)

	if len(q.Order) > 0 {
		_sql += " ORDER BY " + strings.Join(q.Order, ", ")
	}

	if q.Paginate != "" {
		_sql += " " + q.Paginate
	}

	rows, err := s.db.Query(_sql, q.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	docs := []*Document{}

	for rows.Next() {
		doc := &Document{}
		jsonEncoded := []byte{}
		rows.Scan(&doc.Serial, &doc.UUID, &jsonEncoded, &doc.CreatedAt, &doc.UpdatedAt)
		json.Unmarshal(jsonEncoded, &doc.Data)
		docs = append(docs, doc)
	}

	res := &Result{
		Totals: totals,
		Hits:   docs,
		Time:   time.Since(now).Seconds(),
	}

	return res, nil
}
