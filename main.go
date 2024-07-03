package main

import (
	"fmt"
	"log"

	"dekel-home-assignment/bigcsv"
)

func main() {
	// Create a new CSVProcessor with a batch size of 10
	processor := bigcsv.NewCSVProcessor(10)

	// Add our FilterEvenAges operation
	processor.AddOperation(bigcsv.FilterEvenAges{})

	// Process the CSV file
	err := processor.Process("input.csv", "output.csv")
	if err != nil {
		log.Fatalf("Error processing CSV: %v", err)
	}

	fmt.Println("CSV processing complete. Results written to output.csv")
}
