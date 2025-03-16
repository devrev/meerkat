package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	// Open DuckDB connection
	db, err := sql.Open("duckdb", "mydata.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a dummy table with sample data
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS dummy_data (
			id INTEGER,
			name VARCHAR,
			value DOUBLE,
			date_created DATE
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert some sample data if the table is empty
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM dummy_data").Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	if count == 0 {
		_, err = db.Exec(`
			INSERT INTO dummy_data VALUES
			(1, 'Item One', 10.5, '2023-01-15'),
			(2, 'Item Two', 20.75, '2023-02-20'),
			(3, 'Item Three', 30.0, '2023-03-25'),
			(4, 'Item Four', 40.25, '2023-04-30'),
			(5, 'Item Five', 50.5, '2023-05-05')
		`)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Signal ready
	fmt.Println("READY")

	// Process commands from stdin
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "QUERY:") {
			// Extract SQL query from command
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				fmt.Println("ERROR: Invalid QUERY command format")
				continue
			}

			sqlQuery := strings.TrimSpace(parts[1])

			// Execute the query
			rows, err := db.Query(sqlQuery)
			if err != nil {
				fmt.Printf("ERROR: %s\n", err.Error())
				continue
			}

			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				fmt.Printf("ERROR: %s\n", err.Error())
				rows.Close()
				continue
			}

			// Prepare result slice
			var results []map[string]interface{}

			// Process rows
			for rows.Next() {
				// Create a slice of interface{} to hold the values
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))

				// Create a pointer to each item in values
				for i := range columns {
					valuePtrs[i] = &values[i]
				}

				// Scan the result into the pointers
				if err := rows.Scan(valuePtrs...); err != nil {
					fmt.Printf("ERROR: %s\n", err.Error())
					rows.Close()
					continue
				}

				// Create a map for this row
				row := make(map[string]interface{})
				for i, col := range columns {
					row[col] = values[i]
				}

				results = append(results, row)
			}

			rows.Close()

			if err = rows.Err(); err != nil {
				fmt.Printf("ERROR: %s\n", err.Error())
				continue
			}

			// Send results as JSON
			resultJSON, err := json.Marshal(results)
			if err != nil {
				fmt.Printf("ERROR: %s\n", err.Error())
				continue
			}

			// Return the results without the "RESULT:" prefix
			fmt.Println(string(resultJSON))
		} else {
			fmt.Println("ERROR: Unknown command. Only QUERY: is supported")
		}
	}
}
