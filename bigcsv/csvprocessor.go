package bigcsv

// CSVProcessor orchestrates the processing of CSV data using a series of operations
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

// AddOperation adds a new operation to the processing pipeline
func (cp *CSVProcessor) AddOperation(op Operation) {
	cp.operations = append(cp.operations, op)
}

// ProcessBatch applies all operations to a single batch of data
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
			break // End of input reached
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
