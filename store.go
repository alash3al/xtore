package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	// the posgres driver
	_ "github.com/lib/pq"
)

// Store handles the store configurations
type Store struct {
	db *sql.DB
}

// NewStore creates new store
func NewStore(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS _store_(
		_serial		SERIAL PRIMARY KEY,
		_uuid		UUID DEFAULT gen_random_uuid(),
		_doc		JSONB,
		_updatedAt 	INTEGER,
		_createdAt 	INTEGER,

		INVERTED INDEX _doc (_doc)
	)`); err != nil {
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
	for k, v := range data {
		doc.Data[k] = v
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

func (s *Store) Filter(q *Query) ([]*Document, error) {
	sql, args := q.Build()

	if sql != "" {
		sql = fmt.Sprintf("SELECT _serial, _uuid, _doc, _createdAt, _updatedAt FROM _store_ WHERE %s", sql)
	} else {
		sql = "SELECT _serial, _uuid, _doc, _createdAt, _updatedAt FROM _store_"
	}

	rows, err := s.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}

	docs := []*Document{}

	for rows.Next() {
		doc := &Document{}
		jsonEncoded := []byte{}
		rows.Scan(&doc.Serial, &doc.UUID, &jsonEncoded, &doc.CreatedAt, &doc.UpdatedAt)
		json.Unmarshal(jsonEncoded, &doc.Data)
		docs = append(docs, doc)
	}

	return docs, nil
}
