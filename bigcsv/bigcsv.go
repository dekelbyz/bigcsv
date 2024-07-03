package bigcsv

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
)

type Table [][]string

type Operation interface {
	Execute(input Table) (Table, error)
}

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

type CSVProcessor struct {
	operations []Operation
	batchSize  int
	handler    CSVHandlerInterface
}

func NewCSVProcessor(batchSize int, handler CSVHandlerInterface) *CSVProcessor {
	return &CSVProcessor{
		batchSize: batchSize,
		handler:   handler,
	}
}

func (cp *CSVProcessor) AddOperation(op Operation) {
	cp.operations = append(cp.operations, op)
}

func (cp *CSVProcessor) ProcessBatch(batch Table) (Table, error) {
	result := batch
	for _, op := range cp.operations {
		var err error
		result, err = op.Execute(result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (cp *CSVProcessor) Process() error {
	defer cp.handler.Close()

	for {
		batch, err := cp.handler.ReadBatch(cp.batchSize)
		if err != nil {
			return err
		}

		if len(batch) == 0 {
			break
		}

		processedBatch, err := cp.ProcessBatch(batch)
		if err != nil {
			return err
		}

		if err := cp.handler.WriteBatch(processedBatch); err != nil {
			return err
		}
	}

	return nil
}

// #### //
type FilterEvenAges struct{}

func (f FilterEvenAges) Execute(input Table) (Table, error) {
	var result Table
	for _, row := range input {
		if len(row) >= 5 {
			age, err := strconv.Atoi(row[4])
			if err == nil && age%2 == 0 {
				result = append(result, row)
			}
		}
	}
	return result, nil
}

type FilterByDepartment struct {
	Department string
}

func (f FilterByDepartment) Execute(input Table) (Table, error) {
	var result Table
	for _, row := range input {
		if len(row) >= 3 && row[2] == f.Department {
			result = append(result, row)
		}
	}
	return result, nil
}
