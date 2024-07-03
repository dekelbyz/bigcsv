package main

import (
	"fmt"
	"log"
	"strconv"

	"dekel-home-assignment/bigcsv"
)

func main() {
	// Initialize CSV handler for input and output files
	handler, err := bigcsv.NewCSVHandler("input.csv", "output.csv")
	if err != nil {
		log.Fatalf("Error creating CSV handler: %v", err)
	}

	// Create a new CSV processor with batch size of 10
	processor := bigcsv.NewCSVProcessor(10, handler)

	// Add operation to filter rows:
	processor.AddOperation(bigcsv.FilterRows{
		Condition: func(record []string) bool {
			department := record[2]
			age, _ := strconv.Atoi(record[4])
			return department == "Engineering" && age > 40
		},
	})

	// Add operation to select specific columns:
	processor.AddOperation(bigcsv.GetColumns{ColumnIndices: []int{0, 1, 5}})

	err = processor.Process()
	if err != nil {
		log.Fatalf("Error processing CSV: %v", err)
	}

	fmt.Println("CSV processing complete. Results written to output.csv")
}