package xtore

// Document is the row meta data
type Document struct {
	Serial    string                 `json:"_serial"`
	UUID      string                 `json:"_uuid"`
	Data      map[string]interface{} `json:"_doc"`
	CreatedAt int64                  `json:"_createdAt"`
	UpdatedAt int64                  `json:"_updatedAt"`
}
