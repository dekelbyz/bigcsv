package main

import (
	"fmt"
	"log"

	"dekel-home-assignment/bigcsv"
)

func main() {
	// Create a CSVHandler
	handler, err := bigcsv.NewCSVHandler("input.csv", "output.csv")
	if err != nil {
		log.Fatalf("Error creating CSV handler: %v", err)
	}

	// Create a CSVProcessor with the handler
	processor := bigcsv.NewCSVProcessor(10, handler) // batch size of 10

	// Add FilterEvenAges operation
	processor.AddOperation(bigcsv.FilterEvenAges{})

	// Add FilterByDepartment operation (let's filter for "Engineering" department)
	processor.AddOperation(bigcsv.FilterByDepartment{Department: "Engineering"})

	// Process the CSV file
	err = processor.Process()
	if err != nil {
		log.Fatalf("Error processing CSV: %v", err)
	}

	fmt.Println("CSV processing complete. Results written to output.csv")
}
