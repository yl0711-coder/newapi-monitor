package monitor

import (
	"encoding/json"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestReparseFundsWithoutHistoricalUnitKeepsExplicitCurrency(t *testing.T) {
	for _, tc := range []struct {
		name, content, currency string
		nativeKnown, usdKnown   bool
	}{
		{"native CNY", "充值金额: ¥20.00，支付金额: 10.00", "CNY", true, false},
		{"explicit USD", "充值金额: $20.00", "USD", true, true},
		{"quota only", "topup", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, legacyVersion := range []any{nil, 3} {
				db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
				if err != nil {
					t.Fatal(err)
				}
				sqlDB, err := db.DB()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = sqlDB.Close() })
				if err := db.AutoMigrate(&ChannelUpstreamFundEvent{}); err != nil {
					t.Fatal(err)
				}
				m := &Monitor{storeDB: db}
				row := ChannelUpstreamAccount{Domain: "funds.example", Provider: upstreamProviderNewAPI, BalanceUnit: 1000000}
				raw, err := json.Marshal(map[string]any{"id": 1, "created_at": 100, "type": 1, "quota": 1000000, "content": tc.content})
				if err != nil {
					t.Fatal(err)
				}
				decodeRow := row
				decodeRow.EconomicUnitUnavailable = true
				item, err := decodeUpstreamFundItem(decodeRow, 1, raw)
				if err != nil {
					t.Fatal(err)
				}
				if item.UnitPerUSD != 0 || item.AmountKnown != tc.usdKnown {
					t.Fatal("decoder substituted current/default quota unit")
				}
				old := makeChannelUpstreamFundEvent(row, newAPIUpstreamAccountEpoch(row), item, 2, 200)
				old.AmountUSD, old.AmountKnown = 0, false
				old.UpstreamAmount, old.UpstreamAmountKnown, old.UpstreamCurrency = 0, false, ""
				if err := db.Create(&old).Error; err != nil {
					t.Fatal(err)
				}
				if err := db.Model(&old).Updates(map[string]any{"unit_per_usd": nil, "parser_version": legacyVersion, "reparse_error": "legacy missing unit"}).Error; err != nil {
					t.Fatal(err)
				}
				if err := m.reparseStoredUpstreamFundEvents(t.Context(), row, 300); err != nil {
					t.Fatal(err)
				}
				var got ChannelUpstreamFundEvent
				if err := db.First(&got).Error; err != nil {
					t.Fatal(err)
				}
				if got.ReparseError != "" || got.ParserVersion != upstreamFundParserVersion || got.UnitPerUSD != 0 || got.AmountKnown != tc.usdKnown || got.UpstreamAmountKnown != tc.nativeKnown || got.UpstreamCurrency != tc.currency {
					t.Fatalf("unexpected evidence flags: %+v", got)
				}
				if tc.nativeKnown && got.UpstreamAmount != 20 || tc.usdKnown && got.AmountUSD != 20 {
					t.Fatal("explicit amount changed")
				}
				if got.RawJSON != old.RawJSON || got.ObservedCount != 2 || got.FetchedAt != old.FetchedAt {
					t.Fatal("original evidence changed")
				}
				if err := m.reparseStoredUpstreamFundEvents(t.Context(), row, 400); err != nil {
					t.Fatal(err)
				}
				var again ChannelUpstreamFundEvent
				if err := db.First(&again).Error; err != nil {
					t.Fatal(err)
				}
				if again != got {
					t.Fatal("reparse is not idempotent")
				}
			}
		})
	}
}

func TestPersistFundCannotInventOrDiscardHistoricalUnit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		old      any
		incoming float64
	}{
		{"NULL to known", nil, 500000},
		{"zero to known", 0, 500000},
		{"known to zero", 500000, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			sqlDB, err := db.DB()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sqlDB.Close() })
			if err := db.AutoMigrate(&ChannelUpstreamFundEvent{}); err != nil {
				t.Fatal(err)
			}
			old := ChannelUpstreamFundEvent{Domain: "funds.example", AccountEpoch: "epoch", EventKey: "1-key", Provider: upstreamProviderNewAPI, AmountUSD: 20, AmountKnown: true, ObservedCount: 1}
			if err := db.Create(&old).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&old).Update("unit_per_usd", tc.old).Error; err != nil {
				t.Fatal(err)
			}
			var before ChannelUpstreamFundEvent
			if err := db.First(&before).Error; err != nil {
				t.Fatal(err)
			}
			incoming := before
			incoming.AmountUSD, incoming.UnitPerUSD = 99, tc.incoming
			m := &Monitor{storeDB: db}
			if err := m.persistUpstreamFundEvents(t.Context(), []ChannelUpstreamFundEvent{incoming}); err != nil {
				t.Fatal(err)
			}
			var got ChannelUpstreamFundEvent
			if err := db.First(&got).Error; err != nil {
				t.Fatal(err)
			}
			if got.UnitPerUSD != before.UnitPerUSD || got.AmountUSD != before.AmountUSD || got.AmountKnown != before.AmountKnown {
				t.Fatal("historical monetary evidence changed")
			}
		})
	}
}
