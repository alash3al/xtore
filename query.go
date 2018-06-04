package main

import (
	"fmt"
	"strings"
)

// QueryElement contains a single query operation
type QueryElement struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	Op    string      `json:"op"`
}

func (qe *QueryElement) Build(seq int) (string, []byte) {
	return fmt.Sprintf("(%s %s $%d)", qe.Key, qe.Op, seq), jsonEncode(qe.Value)
}

// Query the query itself
type Query struct {
	And []QueryElement `json:"and"`
	Or  []QueryElement `json:"or"`
}

func (q *Query) Build() (string, []interface{}) {
	sql := ""
	andSQL := []string{}
	orSQL := []string{}
	args := []interface{}{}

	for _, qe := range q.And {
		sql, arg := qe.Build(len(args) + 1)
		andSQL = append(andSQL, sql)
		args = append(args, arg)
	}

	for _, qe := range q.Or {
		sql, arg := qe.Build(len(args) + 1)
		orSQL = append(orSQL, sql)
		args = append(args, arg)
	}

	if len(andSQL) > 0 && len(orSQL) > 0 {
		sql = fmt.Sprintf(
			"(%s) AND (%s)",
			strings.Join(andSQL, " AND "),
			strings.Join(orSQL, " OR "),
		)
	} else if len(andSQL) > 0 {
		sql = strings.Join(andSQL, " AND ")
	} else if len(orSQL) > 0 {
		sql = strings.Join(orSQL, " OR ")
	}

	return sql, args
}
