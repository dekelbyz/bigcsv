package bigcsv

type FilterRows struct {
	Condition func(record []string) bool
}

func (fr FilterRows) Execute(input Table) (Table, error) {
	var result Table
	for _, row := range input {
		if fr.Condition(row) {
			result = append(result, row)
		}
	}
	return result, nil
}

type GetColumns struct {
	ColumnIndices []int
}

func (gc GetColumns) Execute(input Table) (Table, error) {
	var result Table
	for _, row := range input {
		newRow := make([]string, 0, len(gc.ColumnIndices))
		for _, index := range gc.ColumnIndices {
			if index < len(row) {
				newRow = append(newRow, row[index])
			} else {
				// You might want to handle this case differently depending on your requirements
				newRow = append(newRow, "")
			}
		}
		result = append(result, newRow)
	}
	return result, nil
}
