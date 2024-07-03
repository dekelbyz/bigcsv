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

	// Process the CSV file line by line
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Apply all operations to the current row
		result := [][]string{record}
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

	writer.Flush()
	return writer.Error()
}
