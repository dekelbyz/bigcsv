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
			if len(record) < 6 {
				return false
			}
			department := record[2]
			age, err := strconv.Atoi(record[4])
			if err != nil {
				return false
			}
			return department == "Engineering" && age > 40
		},
	})

	// Get the salary column (index 5)
	processor.AddOperation(bigcsv.GetColumn{ColumnIndex: 5})

	// Process the CSV file
	err = processor.Process()
	if err != nil {
		log.Fatalf("Error processing CSV: %v", err)
	}

	fmt.Println("CSV processing complete. Results written to output.csv")
	fmt.Println("The output file contains salaries of Engineering employees over 40 years old.")
}
