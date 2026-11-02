package config

import (
	"fmt"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// DomainPrices converts the fixture prices into validated domain
// observations; every string money value is parsed, never converted
// through a float.
func (d DispatchDevelopment) DomainPrices() ([]domain.Price, error) {
	return domainPrices("dispatch.development.prices", d.Prices)
}

func domainPrices(name string, fixtures []PriceFixture) ([]domain.Price, error) {
	out := make([]domain.Price, 0, len(fixtures))
	for i, f := range fixtures {
		bound, err := domain.ParseMoney(f.Currency, f.MaxExposure)
		if err != nil {
			return nil, fmt.Errorf("%s[%d].max_exposure: %w", name, i, err)
		}
		p := domain.Price{Revision: f.Revision, Kind: domain.DispatchKind(f.Kind), Route: f.Route, Provider: f.Provider, Model: f.Model, Currency: f.Currency, EffectiveFrom: f.EffectiveFrom.UTC(), EffectiveUntil: f.EffectiveUntil.UTC(), PerMillion: map[domain.UsageCategory]int64{}, MaxExposure: bound.Amount}
		p.InputIncludesCached, p.OutputIncludesReasoning = f.InputIncludesCached, f.OutputIncludesReasoning
		for category, value := range f.PerMillion {
			m, err := domain.ParseMoney(f.Currency, value)
			if err != nil {
				return nil, fmt.Errorf("%s[%d].per_million.%s: %w", name, i, category, err)
			}
			p.PerMillion[domain.UsageCategory(category)] = m.Amount
		}
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", name, i, err)
		}
		out = append(out, p)
	}
	return out, nil
}

// LoadPriceBook reads a reviewed ENV-06 price book file (P2.1): schemaVersion
// 1 and a prices list in the fixture format (scale-6 decimal strings per one
// million units, RFC 3339 interval). Unknown keys reject the file.
func LoadPriceBook(path string) ([]domain.Price, error) {
	k := koanf.New(".")
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("dispatch.prices.initial_file %s: %w", path, err)
	}
	var book struct {
		SchemaVersion int            `koanf:"schemaVersion"`
		Prices        []PriceFixture `koanf:"prices"`
	}
	if err := k.UnmarshalWithConf("", &book, koanf.UnmarshalConf{DecoderConfig: &mapstructure.DecoderConfig{
		DecodeHook:  mapstructure.ComposeDecodeHookFunc(mapstructure.StringToTimeHookFunc(time.RFC3339)),
		ErrorUnused: true, WeaklyTypedInput: true, Result: &book,
	}}); err != nil {
		return nil, fmt.Errorf("dispatch.prices.initial_file %s: %w", path, err)
	}
	if book.SchemaVersion != 1 || len(book.Prices) == 0 {
		return nil, fmt.Errorf("dispatch.prices.initial_file %s: needs schemaVersion 1 and at least one price", path)
	}
	return domainPrices("dispatch.prices.initial_file prices", book.Prices)
}
