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

type GetColumn struct {
	ColumnIndex int
}

func (gc GetColumn) Execute(input Table) (Table, error) {
	var result Table
	for _, row := range input {
		if gc.ColumnIndex < len(row) {
			result = append(result, []string{row[gc.ColumnIndex]})
		}
	}
	return result, nil
}
