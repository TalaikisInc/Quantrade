package models

import (
	"encoding/json"
)

type System struct {
	ID          int
	Title       string
	Description string
	Slug        string
	Indicator   Indicator
}

type SystemJSON struct {
	ID          int       `json:"name, omitempty"`
	Title       string    `json:"title, omitempty"`
	Description string    `json:"description, omitempty"`
	Slug        string    `json:"slug, omitempty"`
	Indicator   Indicator `json:"indicator, omitempty"`
}

func (p *System) MarshalJSON() ([]byte, error) {
	return json.Marshal(SystemJSON{
		p.ID,
		p.Title,
		p.Description,
		p.Slug,
		p.Indicator,
	})
}

func (p *System) UnmarshalJSON(b []byte) error {
	temp := &SystemJSON{}

	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}

	p.ID = temp.ID
	p.Title = temp.Title
	p.Description = temp.Description
	p.Slug = temp.Slug
	p.Indicator = temp.Indicator

	return nil
}
