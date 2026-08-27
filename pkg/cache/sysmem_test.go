package cache

import (
	"testing"
)

func TestAvailableMemoryAndDefaultBudget(t *testing.T) {
	avail := AvailableMemory()
	if avail <= 0 {
		t.Fatalf("AvailableMemory() = %d, want > 0", avail)
	}

	budgetLimit := DefaultBudgetLimit()
	if budgetLimit <= 0 {
		t.Fatalf("DefaultBudgetLimit() = %d, want > 0", budgetLimit)
	}

	if budgetLimit != avail/2 {
		t.Fatalf("DefaultBudgetLimit() = %d, want %d (50%% of %d)", budgetLimit, avail/2, avail)
	}
}
