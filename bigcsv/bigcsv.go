package bigcsv

import (
	"encoding/csv"
	"io"
	"os"
)

type Operation interface {
	Execute(input [][]string) ([][]string, error)
}

// CSVHandler handles reading from and writing to CSV files
type CSVHandler struct {
	reader     *csv.Reader
	writer     *csv.Writer
	inputFile  *os.File
	outputFile *os.File
}

func NewCSVHandler(inputPath, outputPath string) (*CSVHandler, error) {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return nil, err
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		inputFile.Close()
		return nil, err
	}

	return &CSVHandler{
		reader:     csv.NewReader(inputFile),
		writer:     csv.NewWriter(outputFile),
		inputFile:  inputFile,
		outputFile: outputFile,
	}, nil
}

func (ch *CSVHandler) ReadBatch(batchSize int) ([][]string, error) {
	batch := make([][]string, 0, batchSize)
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

func (ch *CSVHandler) WriteBatch(batch [][]string) error {
	for _, record := range batch {
		if err := ch.writer.Write(record); err != nil {
			return err
		}
	}
	return nil
}

func (ch *CSVHandler) Close() error {
	ch.writer.Flush()
	if err := ch.writer.Error(); err != nil {
		return err
	}
	if err := ch.inputFile.Close(); err != nil {
		return err
	}
	return ch.outputFile.Close()
}

type CSVProcessor struct {
	operations []Operation
	batchSize  int
}

func NewCSVProcessor(batchSize int) *CSVProcessor {
	return &CSVProcessor{
		batchSize: batchSize,
	}
}

func (cp *CSVProcessor) AddOperation(op Operation) {
	cp.operations = append(cp.operations, op)
}

func (cp *CSVProcessor) Process(inputPath, outputPath string) error {
	handler, err := NewCSVHandler(inputPath, outputPath)
	if err != nil {
		return err
	}
	defer handler.Close()

	for {
		batch, err := handler.ReadBatch(cp.batchSize)
		if err != nil {
			return err
		}

		if len(batch) == 0 {
			break
		}

		// Apply operations
		for _, op := range cp.operations {
			batch, err = op.Execute(batch)
			if err != nil {
				return err
			}
		}

		if err := handler.WriteBatch(batch); err != nil {
			return err
		}
	}

	return nil
}
