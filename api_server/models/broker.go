package models

import (
	"encoding/json"
)

type Broker struct {
	ID              int
	Title           string
	Description     string
	Slug            string
	RegistrationUrl string
}

type BrokerJSON struct {
	ID              int    `json:"id, omitempty"`
	Title           string `json:"title, omitempty"`
	Description     string `json:"description, omitempty"`
	Slug            string `json:"slug, omitempty"`
	RegistrationUrl string `json:"registration_url, omitempty"`
}

func (p *Broker) MarshalJSON() ([]byte, error) {
	return json.Marshal(BrokerJSON{
		p.ID,
		p.Title,
		p.Description,
		p.Slug,
		p.RegistrationUrl,
	})
}

func (p *Broker) UnmarshalJSON(b []byte) error {
	temp := &BrokerJSON{}

	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}

	p.ID = temp.ID
	p.Title = temp.Title
	p.Description = temp.Description
	p.Slug = temp.Slug
	p.RegistrationUrl = temp.RegistrationUrl

	return nil
}
