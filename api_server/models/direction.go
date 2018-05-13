package models

import (
	"encoding/json"
)

type Direction struct {
	ID          int
	Description string
}

type DirectionJSON struct {
	ID          int    `json:"id, omitempty"`
	Description string `json:"description, omitempty"`
}

func (p *Direction) MarshalJSON() ([]byte, error) {
	return json.Marshal(DirectionJSON{
		p.ID,
		p.Description,
	})
}

func (p *Direction) UnmarshalJSON(b []byte) error {
	temp := &DirectionJSON{}

	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}

	p.ID = temp.ID
	p.Description = temp.Description

	return nil
}
