package bigcsv

import (
	"encoding/csv"
	"io"
	"os"
)

type Operation interface {
	Execute(input [][]string) ([][]string, error)
}

type CSVProcessor struct {
	operations []Operation
	batchSize  int // New field to determine how many rows to process at once
}

// New constructor function to create a CSVProcessor with a specified batch size
func NewCSVProcessor(batchSize int) *CSVProcessor {
	return &CSVProcessor{
		batchSize: batchSize,
	}
}

func (cp *CSVProcessor) AddOperation(op Operation) {
	cp.operations = append(cp.operations, op)
}

func (cp *CSVProcessor) Process(inputFile, outputFile string) error {
	// Open input file
	input, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer input.Close()

	// Create output file
	output, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer output.Close()

	// Create CSV reader and writer
	reader := csv.NewReader(input)
	writer := csv.NewWriter(output)
	defer writer.Flush() // Ensure all data is written before closing

	// Process the CSV file in batches
	for {
		// Read a batch of records
		batch := make([][]string, 0, cp.batchSize)
		for i := 0; i < cp.batchSize; i++ {
			record, err := reader.Read()
			if err == io.EOF {
				break // End of file reached
			}
			if err != nil {
				return err // Other error occurred
			}
			batch = append(batch, record)
		}

		// If batch is empty, we've reached the end of the file
		if len(batch) == 0 {
			break
		}

		// Apply all operations to the current batch
		result := batch
		for _, op := range cp.operations {
			result, err = op.Execute(result)
			if err != nil {
				return err
			}
		}

		// Write the result to the output file
		for _, row := range result {
			if err := writer.Write(row); err != nil {
				return err
			}
		}
	}

	return writer.Error() // Return any error that occurred during writing
}
