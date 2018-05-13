package models

import (
	"encoding/json"
	//"reflect"

	"../utils"
)

type Symbol struct {
	ID            int
	Symbol        string
	Description   string
	Currency      string
	Sources       Source
	Digits        int
	ProfitType    int
	Spread        float64
	TickValue     float64
	TickSize      float64
	Price         float64
	Commission    float64
	MarginInitial float64
	Broker        Broker
}

type SymbolJSON struct {
	ID            int    `json:"id, omitempty"`
	Symbol        string `json:"symbol, omitempty"`
	Description   string `json:"description"`
	Currency      string `json:"currency, omitempty"`
	Sources       Source `json:"data_source, omitempty"`
	Digits        string `json:"digits, omitempty"`
	ProfitType    string `json:"profit_type, omitempty"`
	Spread        string `json:"spread, omitempty"`
	TickValue     string `json:"tick_value, omitempty"`
	TickSize      string `json:"tick_size, omitempty"`
	Price         string `json:"price, omitempty"`
	Commission    string `json:"commission, omitempty"`
	MarginInitial string `json:"margin, omitempty"`
	Broker        Broker `json:"broker, omitempty"`
}

func (p *Symbol) MarshalJSON() ([]byte, error) {
	return json.Marshal(SymbolJSON{
		p.ID,
		p.Symbol,
		p.Description,
		p.Currency,
		p.Sources,
		utils.RenderInteger("#.", p.Digits),
		utils.RenderInteger("#.", p.ProfitType),
		utils.RenderFloat("#.", p.Spread),
		utils.RenderFloat("#,###.#####", p.TickValue),
		utils.RenderFloat("#,###.#####", p.TickSize),
		utils.RenderFloat("#,###.#####", p.Price),
		utils.RenderFloat("#,###.##", p.Commission),
		utils.RenderFloat("#,###.##", p.MarginInitial),
		p.Broker,
	})
}
