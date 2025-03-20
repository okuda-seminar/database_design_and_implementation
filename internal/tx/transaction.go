package tx

import "database_design_and_implementation/internal/file"

// Transaction defines an interface for transactions
type Transaction interface {
	Commit() error
	Rollback() error
	Pin(blk file.BlockId)
	Unpin(blk file.BlockId)
	SetInt(blk file.BlockId, offset int, val int, logging bool)
}

// ConcreteTransaction implements the Transaction interface
type ConcreteTransaction struct {
	ID int
}

// Ensure ConcreteTransaction implements Transaction
var _ Transaction = (*ConcreteTransaction)(nil)

func (tx *ConcreteTransaction) Commit() error                                              { return nil }
func (tx *ConcreteTransaction) Rollback() error                                            { return nil }
func (tx *ConcreteTransaction) Pin(blk file.BlockId)                                       {}
func (tx *ConcreteTransaction) Unpin(blk file.BlockId)                                     {}
func (tx *ConcreteTransaction) SetInt(blk file.BlockId, offset int, val int, logging bool) {}
