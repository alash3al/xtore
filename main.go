package main

import (
	"fmt"
	"log"
)

func main() {
	db, err := NewStore("postgresql://root@limitless:26257/sstore1?sslmode=disable")

	if err != nil {
		log.Fatal(err.Error())
		return
	}

	// fmt.Println(db.Insert(map[string]interface{}{
	// 	"s":  "text",
	// 	"i":  12,
	// 	"as": []string{"a1", "a2"},
	// 	"ai": []int{1, 3, 5, 5, 6},
	// }))

	// fmt.Println(db.Get("3a26c499-055b-495c-b9b8-9d1b123bf2c3"))

	// fmt.Println(db.Update("3a26c499-055b-495c-b9b8-9d1b123bf2c3", map[string]interface{}{
	// 	"k1":   "akakakaka",
	// 	"tags": []string{"tag1", "tag2"},
	// }))

	fmt.Println(
		db.Filter(&Query{
		// And: []QueryElement{
		// 	{Key: "_doc->'i'", Value: 12, Op: "="},
		// 	{Key: "tags->'tags'", Value: []string{"tag1"}, Op: "@>"},
		// },
		// Or: []QueryElement{
		// 	{Key: "_doc->'i'", Value: 12, Op: "="},
		// 	// {Key: "tags->'tags'", Value: []string{"tag1"}, Op: "@>"},
		// },
		}),
	)
}
