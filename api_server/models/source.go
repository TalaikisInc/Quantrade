package models

import (
	"encoding/json"
)

type Source struct {
	ID          int
	Description string
}

type SourceJSON struct {
	ID          int    `json:"id, omitempty"`
	Description string `json:"description, omitempty"`
}

func (p *Source) MarshalJSON() ([]byte, error) {
	return json.Marshal(SourceJSON{
		p.ID,
		p.Description,
	})
}

func (p *Source) UnmarshalJSON(b []byte) error {
	temp := &SourceJSON{}

	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}

	p.ID = temp.ID
	p.Description = temp.Description

	return nil
}
