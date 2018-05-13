package models

import (
	"encoding/json"
)

type Indicator struct {
	ID          int
	Title       string
	Description string
	Slug        string
}

type IndicatorJSON struct {
	ID          int    `json:"name, omitempty"`
	Title       string `json:"title, omitempty"`
	Description string `json:"description, omitempty"`
	Slug        string `json:"slug, omitempty"`
}

func (p *Indicator) MarshalJSON() ([]byte, error) {
	return json.Marshal(IndicatorJSON{
		p.ID,
		p.Title,
		p.Description,
		p.Slug,
	})
}

func (p *Indicator) UnmarshalJSON(b []byte) error {
	temp := &IndicatorJSON{}

	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}

	p.ID = temp.ID
	p.Title = temp.Title
	p.Description = temp.Description
	p.Slug = temp.Slug

	return nil
}
