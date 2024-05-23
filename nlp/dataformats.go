package nlp

type Digest struct {
	Summary string `json:"summary,omitempty" bson:"summary,omitempty" jsonschema_description:"A concise summary of the document"`
	Topic   string `json:"topic,omitempty" bson:"topic,omitempty" jsonschema_description:"The topic of the content such as: Threat Intelligence, New Malware, Israel Hamas War, iPhone Release, LLAMA Performance, Disease, Politics, Drug Epidemic, Entertainment, Gaiming etc."`
}

type keyConceptList struct {
	Items []KeyConcept `json:"concepts" jsonschema_description:"Array of keyconcepts"`
}

type KeyConcept struct {
	KeyPhrase   string `json:"keyphrase" jsonschema_description:"'keyphrase' can be the name of a company, product, person, place, security vulnerability, entity, location, organization, object, condition, acronym, documents, service, disease, medical condition, vehicle, polical group etc."`
	Event       string `json:"event" jsonschema_description:"'event' can be action, state or condition associated to the 'keyphrase' such as: what is the 'keyphrase' doing OR what is happening to the 'keyphrase' OR how is 'keyphrase' being impacted."`
	Description string `json:"description"  jsonschema_description:"A concise summary of the 'event' associated to the 'keyphrase'"`
}
