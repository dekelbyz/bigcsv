package main

import (
	"fmt"
	"log"
	"strconv"

	"dekel-home-assignment/bigcsv"
)

func main() {
	handler, err := bigcsv.NewCSVHandler("input.csv", "output.csv")
	if err != nil {
		log.Fatalf("Error creating CSV handler: %v", err)
	}

	processor := bigcsv.NewCSVProcessor(10, handler) // batch size of 10

	// Filter rows for Engineering department and age > 40
	processor.AddOperation(bigcsv.FilterRows{
		Condition: func(record []string) bool {
			department := record[2]
			age, _ := strconv.Atoi(record[4])
			return department == "Engineering" && age > 40
		},
	})
	processor.AddOperation(bigcsv.GetColumns{ColumnIndices: []int{0, 1, 5}})

	// Process the CSV file
	err = processor.Process()
	if err != nil {
		log.Fatalf("Error processing CSV: %v", err)
	}

	fmt.Println("CSV processing complete. Results written to output.csv")
}
