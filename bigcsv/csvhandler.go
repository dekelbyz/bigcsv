package bigcsv

import (
	"encoding/csv"
	"io"
	"os"
)

type CSVHandlerInterface interface {
	ReadBatch(batchSize int) (Table, error)
	WriteBatch(batch Table) error
	Close() error
}

// single responsibility
type CSVHandler struct {
	reader *csv.Reader
	writer *csv.Writer
	input  *os.File
	output *os.File
}

func NewCSVHandler(inputFile, outputFile string) (*CSVHandler, error) {
	input, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}

	output, err := os.Create(outputFile)
	if err != nil {
		input.Close()
		return nil, err
	}

	return &CSVHandler{
		reader: csv.NewReader(input),
		writer: csv.NewWriter(output),
		input:  input,
		output: output,
	}, nil
}

// open closed
func (ch *CSVHandler) ReadBatch(batchSize int) (Table, error) {
	batch := make(Table, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		record, err := ch.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		batch = append(batch, record)
	}
	return batch, nil
}

func (ch *CSVHandler) WriteBatch(batch Table) error {
	for _, row := range batch {
		if err := ch.writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func (ch *CSVHandler) Close() error {
	ch.writer.Flush()
	if err := ch.input.Close(); err != nil {
		return err
	}
	return ch.output.Close()
}