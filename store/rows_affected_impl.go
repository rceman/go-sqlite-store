package store

// validateStatementRequirements validates requirements that are independent of
// SQLite execution. RequireRowsAffected==0 disables the expectation.
func validateStatementRequirements(statementIndex int, st Statement) error {
	if st.RequireRowsAffected < 0 {
		return &RowsAffectedMismatchError{
			Statement: statementIndex,
			Required:  st.RequireRowsAffected,
			Actual:    0,
		}
	}
	return nil
}

func validateRowsAffected(statementIndex int, st Statement, result ExecResult) error {
	if st.RequireRowsAffected > 0 && result.RowsAffected != st.RequireRowsAffected {
		return &RowsAffectedMismatchError{
			Statement: statementIndex,
			Required:  st.RequireRowsAffected,
			Actual:    result.RowsAffected,
		}
	}
	return nil
}
