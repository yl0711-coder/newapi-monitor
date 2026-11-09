package monitor

import "strings"

// Keep authoritative source facts separate from the customer-only cent view.
// Backend evidence/continuity readers must not receive rounded customer money.
type monthlyCustomerBill struct {
	source    monthlyStatement
	statement monthlyStatement
	retained  *monthlyRetainedCentBill
	simple    bool // issued by the simple generator: customer statement layout
	sub       *simpleSubscriptionBill
}

func monthlyReadCustomerBill(record PortalMonthlyBill) (monthlyCustomerBill, error) {
	if strings.HasPrefix(record.PublicationID, monthlyCentBillPublicationPrefix) {
		retained, err := monthlyReadCentBill(record)
		if err != nil {
			return monthlyCustomerBill{}, err
		}
		view, err := monthlyCentBillView(retained)
		if err != nil {
			return monthlyCustomerBill{}, err
		}
		view.Publication = monthlyBillVersion(record)
		return monthlyCustomerBill{source: retained.Source, statement: monthlyStatement(view), retained: &retained}, nil
	}
	p, err := readLegacyMonthlyBill(record)
	if err != nil {
		return monthlyCustomerBill{}, err
	}
	return monthlyCustomerBill{source: p, statement: p}, nil
}

func (bill monthlyCustomerBill) summary() (monthlyStatementRow, error) {
	if bill.retained != nil {
		return monthlyCentBillSummary(*bill.retained)
	}
	return monthlySummary(bill.source), nil
}

func (bill monthlyCustomerBill) workbook() ([]byte, error) {
	if bill.retained != nil {
		return monthlyCentBillXLSX(*bill.retained)
	}
	if bill.simple {
		return simpleStatementXLSX(bill.source, bill.sub)
	}
	return monthlyStatementXLSX(bill.source)
}
