package models

import (
	"encoding/json"
)

type Period struct {
	ID     int
	Period int
	Name   string
}

type PeriodJSON struct {
	ID     int    `json:"id, omitempty"`
	Period int    `json:"period, omitempty"`
	Name   string `json:"name, omitempty"`
}

func (p *Period) MarshalJSON() ([]byte, error) {
	return json.Marshal(PeriodJSON{
		p.ID,
		p.Period,
		p.Name,
	})
}

func (p *Period) UnmarshalJSON(b []byte) error {
	temp := &PeriodJSON{}

	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}

	p.ID = temp.ID
	p.Period = temp.Period
	p.Name = temp.Name

	return nil
}
