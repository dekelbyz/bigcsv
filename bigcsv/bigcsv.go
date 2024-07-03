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

	reader := csv.NewReader(input)
	writer := csv.NewWriter(output)
	defer writer.Flush() // Ensure all data is written before closing

	// Process the CSV file in batches
	for {
		batch := make([][]string, 0, cp.batchSize)
		for i := 0; i < cp.batchSize; i++ {
			record, err := reader.Read()
			if err == io.EOF {
				break // End of file reached
			}
			if err != nil {
				return err
			}
			batch = append(batch, record)
		}

		// If empty = end of file
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

		for _, row := range result {
			if err := writer.Write(row); err != nil {
				return err
			}
		}
	}

	return writer.Error()
}
