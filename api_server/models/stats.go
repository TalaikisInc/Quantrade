package models

import (
	"encoding/json"
)

type Stats struct {
	ID              int
	Broker          Broker
	Symbol          Symbol
	Period          Period
	System          System
	Direction       Direction
	IntradayDD      float32
	MaxDD           float32
	Sharpe          float32
	SharpeBuyHold   float32
	Sortino         float32
	SortinoBuyHold  float32
	Std             float32
	Var             float32
	AvgTrade        float32
	AvgWin          float32
	AvgLoss         float32
	WinRate         float32
	Trades          int
	Fitness         float32
	TotalProfit     float32
	AccMinimum      float32
	Yearly          float32
	YearlyInPerc    float32
	StrategyUrl     string
	Heatmap         string
	StrategyImg     string
	YearlyReturnImg string
}

type StatsJSON struct {
	ID              int       `json:"id, omitempty"`
	Broker          Broker    `json:"broker, omitempty"`
	Symbol          Symbol    `json:"symbol, omitempty"`
	Period          Period    `json:"period, omitempty"`
	System          System    `json:"system, omitempty"`
	Direction       Direction `json:"direction, omitempty"`
	IntradayDD      float32   `json:"intraday_dd"`
	MaxDD           float32   `json:"max_dd"`
	Sharpe          float32   `json:"sharpe"`
	SharpeBuyHold   float32   `json:"sharpe_bh"`
	Sortino         float32   `json:"sortino"`
	SortinoBuyHold  float32   `json:"sortino_bh"`
	Std             float32   `json:"std"`
	Var             float32   `json:"var"`
	AvgTrade        float32   `json:"avg_trade"`
	AvgWin          float32   `json:"avg_win"`
	AvgLoss         float32   `json:"avg_loss"`
	WinRate         float32   `json:"win_rate"`
	Trades          int       `json:"trades"`
	Fitness         float32   `json:"fitness"`
	TotalProfit     float32   `json:"total_profit"`
	AccMinimum      float32   `json:"acc_minimum"`
	Yearly          float32   `json:"yearly"`
	YearlyInPerc    float32   `json:"yearly_p"`
	StrategyUrl     string    `json:"strategy_url"`
	Heatmap         string    `json:"heatmap"`
	StrategyImg     string    `json:"strategy_img"`
	YearlyReturnImg string    `json:"yearly_img"`
}

func (p *Stats) MarshalJSON() ([]byte, error) {
	return json.Marshal(StatsJSON{
		p.ID,
		p.Broker,
		p.Symbol,
		p.Period,
		p.System,
		p.Direction,
		p.IntradayDD,
		p.MaxDD,
		p.Sharpe,
		p.SharpeBuyHold,
		p.Sortino,
		p.SortinoBuyHold,
		p.Std,
		p.Var,
		p.AvgTrade,
		p.AvgWin,
		p.AvgLoss,
		p.WinRate,
		p.Trades,
		p.Fitness,
		p.TotalProfit,
		p.AccMinimum,
		p.Yearly,
		p.YearlyInPerc,
		p.StrategyUrl,
		p.Heatmap,
		p.StrategyImg,
		p.YearlyReturnImg,
	})
}

func (p *Stats) UnmarshalJSON(b []byte) error {
	temp := &StatsJSON{}

	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}

	p.ID = temp.ID
	p.Broker = temp.Broker
	p.Symbol = temp.Symbol
	p.Period = temp.Period
	p.System = temp.System
	p.Direction = temp.Direction
	p.IntradayDD = temp.IntradayDD
	p.MaxDD = temp.MaxDD
	p.Sharpe = temp.Sharpe
	p.SharpeBuyHold = temp.SharpeBuyHold
	p.Sortino = temp.Sortino
	p.SortinoBuyHold = temp.SortinoBuyHold
	p.Std = temp.Std
	p.Var = temp.Var
	p.AvgTrade = temp.AvgTrade
	p.AvgWin = temp.AvgWin
	p.AvgLoss = temp.AvgLoss
	p.WinRate = temp.WinRate
	p.Trades = temp.Trades
	p.Fitness = temp.Fitness
	p.TotalProfit = temp.TotalProfit
	p.AccMinimum = temp.AccMinimum
	p.Yearly = temp.Yearly
	p.YearlyInPerc = temp.YearlyInPerc
	p.StrategyUrl = temp.StrategyUrl
	p.Heatmap = temp.Heatmap
	p.StrategyImg = temp.StrategyImg
	p.YearlyReturnImg = temp.YearlyReturnImg

	return nil
}
